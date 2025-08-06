import asyncio
import os
import json
import lmdb
import logging
import time
from pathlib import Path
from dataclasses import asdict
from typing import Union, Optional
from raftengine.api.log_api import LogRec, LogAPI, RecordCode, LogStats
from raftengine.api.snapshot_api import SnapShot
from raftengine.api.types import ClusterConfig, NodeRec, ClusterSettings


class Records:
    """Low-level LMDB operations and transaction management for Raft log storage."""

    default_map_size=10**9 * 10  # 10GB initial map size
    
    def __init__(self, filepath: os.PathLike, map_size=None):
        # Log record indexes start at 1, per raftengine spec
        self.filepath = Path(filepath).resolve()
        self.map_size = self.default_map_size
        if map_size is not None:
            self.map_size = map_size
        self.env = None
        self.records_db = None      # Sub-DB for log entries
        self.stats_db = None        # Sub-DB for stats and metadata
        self.nodes_db = None        # Sub-DB for cluster nodes
        self.settings_db = None     # Sub-DB for cluster settings
        self.snapshots_db = None    # Sub-DB for snapshot metadata
        self.timestamps_db = None   # Sub-DB for record timestamps
        
        # Raft state variables - initialized from database or defaults
        self.max_index = 0
        self.term = 0
        self.voted_for = None
        self.broken = False
        self.max_commit = 0
        self.max_apply = 0
        self.snapshot = None
        
        # Don't call open from here, we may be in the wrong thread,
        # at least in testing. Maybe in real server if threading is used.
        # Let it get called when the running server is trying to use it.

    def is_open(self):
        return self.env is not None

    def open(self) -> None:
        """Initialize LMDB environment and load existing state."""
        # Create LMDB environment with multiple sub-databases
        self.env = lmdb.Environment(
            str(self.filepath),
            map_size=self.map_size,
            max_dbs=6,           # 6 named databases
            sync=True,           # Force sync to disk
            writemap=True        # Use writeable memory map
        )
        
        # Open all sub-databases
        with self.env.begin(write=True) as txn:
            self.records_db = self.env.open_db(b'records', txn=txn, integerkey=True)
            self.stats_db = self.env.open_db(b'stats', txn=txn)
            self.nodes_db = self.env.open_db(b'nodes', txn=txn)
            self.settings_db = self.env.open_db(b'settings', txn=txn)
            self.snapshots_db = self.env.open_db(b'snapshots', txn=txn)
            self.timestamps_db = self.env.open_db(b'timestamps', txn=txn, integerkey=True)
        
        # Load existing state or initialize defaults
        with self.env.begin() as txn:
            stats_bytes = txn.get(b'stats', db=self.stats_db)
            if stats_bytes:
                stats = json.loads(stats_bytes.decode('utf-8'))
                self.max_index = stats['max_index']
                self.term = stats['term']
                self.voted_for = stats['voted_for']
                self.broken = stats['broken']
                self.max_commit = stats['max_commit']
                self.max_apply = stats['max_apply']
            else:
                # Initialize with defaults
                self.max_index = 0
                self.term = 0
                self.voted_for = None
                self.broken = False
                self.max_commit = 0
                self.max_apply = 0
                with self.env.begin(write=True) as write_txn:
                    self._save_stats(write_txn)
            
            # Load snapshot if exists
            snapshot_bytes = txn.get(b'snapshot', db=self.snapshots_db)
            if snapshot_bytes:
                snapshot_data = json.loads(snapshot_bytes.decode('utf-8'))
                self.snapshot = SnapShot(index=snapshot_data['index'], term=snapshot_data['term'])

    def close(self) -> None:
        """Close LMDB environment and cleanup resources."""
        if self.env is not None:
            self.env.close()
            self.env = None
            
        self.records_db = None
        self.stats_db = None
        self.nodes_db = None
        self.settings_db = None
        self.snapshots_db = None
        self.timestamps_db = None

    def _serialize_rec(self, rec: LogRec) -> bytes:
        """Serialize LogRec to bytes for storage."""
        return json.dumps(asdict(rec)).encode('utf-8')

    def _deserialize_rec(self, data: bytes) -> LogRec:
        """Deserialize bytes to LogRec."""
        return LogRec.from_dict(json.loads(data.decode('utf-8')))

    def _save_stats(self, txn):
        """Save Raft state to stats database."""
        stats = {
            'max_index': self.max_index,
            'term': self.term,
            'voted_for': self.voted_for,
            'broken': self.broken,
            'max_commit': self.max_commit,
            'max_apply': self.max_apply
        }
        txn.put(b'stats', json.dumps(stats).encode('utf-8'), db=self.stats_db)

    def save_entry(self, entry: LogRec) -> LogRec:
        """Save a log entry and update related state."""
        if self.env is None:
            self.open() # pragma: no cover
            
        with self.env.begin(write=True) as txn:
            # Handle index assignment like SQLite implementation
            if (entry.index is None and self.snapshot 
                and self.snapshot.index == self.max_index):
                # First insert after snapshot, have to fix index
                entry.index = self.max_index + 1
                
            if entry.index is not None:
                # Replace existing entry
                key = entry.index.to_bytes(8, 'big')
            else:
                # Auto-assign next index
                entry.index = self.max_index + 1
                key = entry.index.to_bytes(8, 'big')
            
            value = self._serialize_rec(entry)
            txn.put(key, value, db=self.records_db)
            
            # Store timestamp for this record  
            timestamp_value = str(time.time()).encode('utf-8')
            txn.put(key, timestamp_value, db=self.timestamps_db)
            
            # Update tracking state
            if entry.index > self.max_index:
                self.max_index = entry.index
            self._save_stats(txn)
            
        return self.read_entry(entry.index)

    def read_entry(self, index: Optional[int] = None) -> Optional[LogRec]:
        """Read a log entry by index."""
        if self.env is None:
            self.open() # pragma: no cover
            
        with self.env.begin() as txn:
            if index is None:
                # Find the maximum index in records
                cursor = txn.cursor(db=self.records_db)
                if cursor.last():
                    key_bytes = cursor.key()
                    index = int.from_bytes(key_bytes, 'big')
                else:
                    return None
            
            key = index.to_bytes(8, 'big')
            data = txn.get(key, db=self.records_db)
            if not data:
                return None
                
            rec = self._deserialize_rec(data)
            return rec

    def get_broken(self):
        if self.env is None:
            self.open() # pragma: no cover
        return self.broken

    def set_broken(self):
        if self.env is None:
            self.open() # pragma: no cover
        self.broken = True
        self.save_stats()

    def set_fixed(self):
        if self.env is None:
            self.open() # pragma: no cover
        self.broken = False
        self.save_stats()

    def save_stats(self):
        if self.env is None:
            self.open() # pragma: no cover
        with self.env.begin(write=True) as txn:
            self._save_stats(txn)

    def set_term(self, value):
        self.term = value
        self.save_stats()

    def set_voted_for(self, value):
        if self.env is None:
            self.open() # pragma: no cover
        self.voted_for = value
        self.save_stats()

    def get_entry_at(self, index):
        if index < 1:
            return None
        return self.read_entry(index)

    def add_entry(self, rec: LogRec) -> LogRec:
        """Add new entry (append)."""
        orig_index = rec.index
        rec.index = None  # Force auto-assignment
        rec = self.save_entry(rec)
        return rec

    def insert_entry(self, rec: LogRec) -> LogRec:
        """Insert entry at specific index (replace)."""
        rec = self.save_entry(rec)
        return rec

    def set_commit_index(self, index):
        if self.env is None:
            self.open() # pragma: no cover
        if index > self.max_commit:
            self.max_commit = index
            self.save_stats()

    def set_apply_index(self, index):
        if self.env is None:
            self.open() # pragma: no cover
        if index > self.max_apply:
            self.max_apply = index
            self.save_stats()

    def delete_all_from(self, index: int):
        """Delete all entries from specified index onwards."""
        if self.env is None:
            self.open() # pragma: no cover
            
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor(db=self.records_db)
            start_key = index.to_bytes(8, 'big')
            
            # Position cursor at first entry >= index
            if cursor.set_range(start_key):
                # Delete all entries from this position forward
                while cursor.delete():
                    pass  # delete() auto-advances on succes
            
            self.max_index =  index - 1 if index > 0 else 0
            self._save_stats(txn)

    def save_cluster_config(self, config: ClusterConfig) -> None:
        """Save cluster configuration to database."""
        if self.env is None:
            self.open() # pragma: no cover
            
        with self.env.begin(write=True) as txn:
            # Clear existing nodes
            cursor = txn.cursor(db=self.nodes_db)
            cursor.first()
            while cursor.delete():
                pass # pragma: no cover   should only ever be one
            
            # Save all nodes
            for node in config.nodes.values():
                node_data = {
                    'uri': node.uri,
                    'is_adding': node.is_adding,
                    'is_removing': node.is_removing
                }
                txn.put(node.uri.encode('utf-8'), 
                       json.dumps(node_data).encode('utf-8'), 
                       db=self.nodes_db)
            
            # Save settings
            settings_data = {
                'heartbeat_period': config.settings.heartbeat_period,
                'election_timeout_min': config.settings.election_timeout_min,
                'election_timeout_max': config.settings.election_timeout_max,
                'max_entries_per_message': config.settings.max_entries_per_message,
                'use_pre_vote': config.settings.use_pre_vote,
                'use_check_quorum': config.settings.use_check_quorum,
                'use_dynamic_config': config.settings.use_dynamic_config,
                'commands_idempotent': config.settings.commands_idempotent
            }
            txn.put(b'settings', json.dumps(settings_data).encode('utf-8'), db=self.settings_db)

    def get_cluster_config(self) -> Optional[ClusterConfig]:
        """Retrieve cluster configuration from database."""
        if self.env is None:
            self.open() # pragma: no cover
            
        with self.env.begin() as txn:
            # Load settings
            settings_bytes = txn.get(b'settings', db=self.settings_db)
            if settings_bytes is None:
                return None
                
            settings_data = json.loads(settings_bytes.decode('utf-8'))
            settings = ClusterSettings(
                heartbeat_period=settings_data['heartbeat_period'],
                election_timeout_min=settings_data['election_timeout_min'],
                election_timeout_max=settings_data['election_timeout_max'],
                max_entries_per_message=settings_data['max_entries_per_message'],
                use_pre_vote=settings_data['use_pre_vote'],
                use_check_quorum=settings_data['use_check_quorum'],
                use_dynamic_config=settings_data['use_dynamic_config'],
                commands_idempotent=settings_data['commands_idempotent']
            )
            
            # Load nodes
            nodes = {}
            cursor = txn.cursor(db=self.nodes_db)
            cursor.first()
            for key, value in cursor:
                node_data = json.loads(value.decode('utf-8'))
                node = NodeRec(
                    uri=node_data['uri'],
                    is_adding=node_data['is_adding'],
                    is_removing=node_data['is_removing']
                )
                nodes[node.uri] = node
            
            return ClusterConfig(nodes=nodes, settings=settings)

    def get_first_index(self):
        """Get the index of the first log entry."""
        if self.env is None:
            self.open() # pragma: no cover

        if self.snapshot:
            if self.max_index > self.snapshot.index:
                return self.snapshot.index + 1
            return None
        if self.max_index > 0:
            return 1
        return None

    def get_last_index(self):
        """Get the index of the last log entry."""
        if self.env is None:
            self.open() # pragma: no cover
            
        if not self.snapshot:
            return self.max_index
        return max(self.max_index, self.snapshot.index)

    async def install_snapshot(self, snapshot: SnapShot):
        """Install snapshot and prune old log entries."""
        if self.env is None:
            self.open() # pragma: no cover

        # installing the snapshot in a separate transactions is fine,
        # when it is done before pruning the records
        with self.env.begin(write=True) as txn:
            # Delete snapshot data first
            cursor = txn.cursor(db=self.snapshots_db) 
            cursor.first()
            while cursor.delete():
                pass
            
            # Save new snapshot
            snapshot_data = {'index': snapshot.index, 'term': snapshot.term}
            txn.put(b'snapshot', json.dumps(snapshot_data).encode('utf-8'), db=self.snapshots_db)

            # Update state
            self.max_index = max(snapshot.index, self.max_index)
            self.snapshot = snapshot

        # limit the number of deletes done in one transaction
        async def delete_100():
            counter = 0
            with self.env.begin(write=True) as txn:
                cursor = txn.cursor(db=self.records_db)
                cursor.first()
                while cursor.key() and counter < 100:
                    key_index = int.from_bytes(cursor.key(), 'big')
                    if key_index > snapshot.index:
                        return counter
                    cursor.delete()
                    counter += 1
            return counter
        
        while await delete_100() > 0:
            pass
            

    def get_snapshot(self):
        """Get current snapshot."""
        if self.env is None:
            self.open() # pragma: no cover
        return self.snapshot


class LmdbLog(LogAPI):
    """LMDB implementation of the LogAPI interface."""
    
    def __init__(self, filepath: os.PathLike, map_size=None):
        self.filepath = filepath
        self.map_size = map_size
        self.records = None
        self.logger = logging.getLogger(__name__)

    async def start(self):
        """Initialize the log storage."""
        # This indirection helps deal with the need to restrict
        # access to a single thread
        self.records = Records(self.filepath, self.map_size)
        if not self.records.is_open():
            self.records.open()

    async def stop(self):
        """Stop the log storage and cleanup resources."""
        if self.records:
            self.records.close()

    async def get_broken(self):
        return self.records.get_broken()

    async def set_broken(self):
        return self.records.set_broken()

    async def set_fixed(self):
        return self.records.set_fixed()

    async def get_term(self) -> Union[int, None]:
        if not self.records.is_open():
            self.records.open() # pragma: no cover
        return self.records.term

    async def set_term(self, value: int):
        self.records.set_term(value)

    async def get_voted_for(self) -> Union[str, None]:
        return self.records.voted_for

    async def set_voted_for(self, value: str):
        self.records.set_voted_for(value)

    async def incr_term(self):
        self.records.set_term(self.records.term + 1)
        return self.records.term

    async def append(self, entry: LogRec) -> LogRec:
        save_rec = LogRec.from_dict(entry.__dict__)
        return_rec = self.records.add_entry(save_rec)
        #self.logger.debug("new log record %s", return_rec.index)
        log_rec = LogRec.from_dict(return_rec.__dict__)
        if return_rec.index <= self.records.max_commit:
            return_rec.committed = True
        if return_rec.index <= self.records.max_apply:
            return_rec.applied = True
        return return_rec

    async def replace(self, entry: LogRec) -> LogRec:
        if entry.index is None:
            raise Exception("api usage error, call append for new record")
        if entry.index < 1:
            raise Exception("api usage error, cannot insert at index less than 1")
        save_rec = LogRec.from_dict(entry.__dict__)
        next_index = self.records.max_index + 1
        if save_rec.index > next_index:
            raise Exception("cannot replace record with index greater than max record index")
        self.records.insert_entry(save_rec)
        log_rec = LogRec.from_dict(save_rec.__dict__)
        if log_rec.index <= self.records.max_commit:
            log_rec.committed = True
        if log_rec.index <= self.records.max_apply:
            log_rec.applied = True
        return log_rec

    async def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if index is None:
            index = self.records.max_index
        else:
            if index < 1:
                raise Exception(f"cannot get index {index}, not in records")
            if index > self.records.max_index:
                return None
        rec = self.records.get_entry_at(index)
        if rec is None:
            return None

        log_rec = LogRec.from_dict(rec.__dict__)
        if log_rec.index <= self.records.max_commit:
            log_rec.committed = True
        if log_rec.index <= self.records.max_apply:
            log_rec.applied = True
        return log_rec

    async def get_last_index(self):
        return self.records.get_last_index()

    async def get_last_term(self):
        rec = self.records.read_entry()
        if rec is None:
            snap = self.records.get_snapshot()
            if snap:
                return snap.term
            return 0
        return rec.term

    async def mark_committed(self, index: int) -> None:
        self.records.set_commit_index(index)

    async def mark_applied(self, index: int) -> None:
        self.records.set_apply_index(index)

    async def get_commit_index(self):
        return self.records.max_commit

    async def get_applied_index(self):
        return self.records.max_apply

    async def delete_all_from(self, index: int):
        return self.records.delete_all_from(index)

    async def save_cluster_config(self, config: ClusterConfig) -> None:
        return self.records.save_cluster_config(config)

    async def get_cluster_config(self) -> Optional[ClusterConfig]:
        return self.records.get_cluster_config()

    async def get_first_index(self) -> Optional[int]:
        return self.records.get_first_index()

    async def install_snapshot(self, snapshot: SnapShot):
        return await self.records.install_snapshot(snapshot)

    async def get_snapshot(self) -> Optional[SnapShot]:
        return self.records.get_snapshot()

    async def get_stats(self) -> LogStats:
        """Get statistics for LmdbLog with percent_remaining calculation."""
        if not self.records.is_open():
            self.records.open()  # pragma: no cover
        
        with self.records.env.begin() as txn:
            # Get record count
            cursor = txn.cursor(db=self.records.records_db)
            record_count = sum(1 for _ in cursor)

            # Calculate records since snapshot
            snapshot_index = self.records.snapshot.index if self.records.snapshot else 0
            records_since_snapshot = max(0, self.records.max_index - snapshot_index)
            
            # Calculate records per minute from timestamps
            records_per_minute = 0.0
            current_time = time.time()
            five_minutes_ago = current_time - 300  # 5 minutes
            
            # Collect recent timestamps
            recent_timestamps = []
            cursor = txn.cursor(db=self.records.timestamps_db)
            for key, value in cursor:
                timestamp = float(value.decode('utf-8'))
                if timestamp >= five_minutes_ago:
                    recent_timestamps.append(timestamp)
            
            # Calculate rate from recent timestamps
            if len(recent_timestamps) >= 2:
                recent_timestamps.sort()
                time_span = recent_timestamps[-1] - recent_timestamps[0]
                if time_span > 0:
                    records_per_minute = (len(recent_timestamps) - 1) * 60.0 / time_span
            
            # Get last record timestamp
            last_record_timestamp = None
            if recent_timestamps:
                last_record_timestamp = max(recent_timestamps)
            
            # Calculate LMDB memory usage and percent remaining
            info = self.records.env.info()
            stat = self.records.env.stat()
            
            map_size = info['map_size']
            used_size = stat['psize'] * info['last_pgno']
            
            percent_remaining = None
            if map_size > 0:
                percent_remaining = max(0.0, min(100.0, (map_size - used_size) * 100.0 / map_size))
            
            return LogStats(
                record_count=record_count,
                records_since_snapshot=records_since_snapshot,
                records_per_minute=records_per_minute,
                percent_remaining=percent_remaining,  # LMDB memory limit tracking
                total_size_bytes=used_size,
                snapshot_index=snapshot_index if snapshot_index > 0 else None,
                last_record_timestamp=last_record_timestamp
            )
