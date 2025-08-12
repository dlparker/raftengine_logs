import abc
import time
import sys
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
import logging
from copy import deepcopy
from raftengine.api.log_api import LogRec, LogAPI, LogStats
from raftengine.api.snapshot_api import SnapShot
from raftengine.api.types import ClusterConfig, NodeRec, ClusterSettings

logger = logging.getLogger(__name__)

class MemoryLog(LogAPI):
    
    def __init__(self):
        self.first_index = None
        self.last_index = 0
        self.last_term = 0
        self.term = 0
        self.entries = {}
        self.voted_for = None
        self.snapshot = None
        self.nodes = None
        self.pending_node = None
        self.cluster_settings = None
        self.broken = False
        self.max_commit = 0
        self.max_apply = 0
        self.max_timestamps = 1000
        
        # Statistics tracking
        self.record_timestamps = []  # Track timestamps for rate calculation
        self.last_record_time = None

    async def close(self):
        self.first_index = None
        self.last_index = 0
        self.last_term = 0
        self.term = 0
        self.entries = {}
        self.voted_for = None
        self.snapshot = None
        self.nodes = None
        self.pending_node = None
        self.cluster_settings = None
        
        # Reset statistics tracking
        self.record_timestamps = []
        self.last_record_time = None
        
    def insert_entry(self, rec: LogRec) -> LogRec:
        if rec.index > self.last_index + 1:
            raise Exception('cannont insert past last index')
        orig_index = rec.index
        if rec.index == 0 or rec.index is None:
            rec.index = self.last_index + 1
        self.entries[rec.index] = rec
        self.last_index = max(rec.index, self.last_index)
        self.last_term = max(rec.term, self.last_term)
        if self.first_index == 0 or self.first_index is None:
            self.first_index = rec.index
        if self.last_index <= rec.index:
            # It is possible for the last record to be re-written
            # with a new term, and we can't tell that from
            # an update to record commit or apply flag, so
            # we'll assume that the term needs to update
            self.last_index = rec.index
            self.last_term = rec.term
            
        # Track timestamp for statistics
        current_time = time.time()
        self.last_record_time = current_time
        self.record_timestamps.append(current_time)
        
        # Keep only last 1000 timestamps for rate calculation (prevent memory growth)
        if len(self.record_timestamps) > self.max_timestamps:
            self.record_timestamps = self.record_timestamps[-self.max_timestamps:]
            
        return rec
            
    def get_entry_at(self, index):
        return self.entries.get(index, None)

    def get_last_entry(self):
        return self.get_entry_at(self.last_index)

    async def delete_ending_with(self, index: int):
        if index == self.last_index:
            self.entries = {}
        else:
            keys = list(self.entries.keys())
            keys.sort()
            for rindex in keys:
                if rindex <= index:
                    del self.entries[rindex]
        if len(self.entries) == 0:
            # this should only be called from install snapshot, so there should be one
            self.first_index = None
            self.last_index = self.snapshot.index
            self.last_term = self.snapshot.term
        else:
            self.first_index = index + 1

    # BEGIN API METHODS
    async def start(self):
        pass

    async def stop(self):
        await self.close()

    async def get_broken(self) -> bool:
        return self.broken
    
    async def set_broken(self) -> None:
        self.broken = True
    
    async def set_fixed(self) -> None:
        self.broken = False
    
    async def get_term(self) -> Union[int, None]:
        return self.term
    
    async def set_term(self, value: int):
        self.term = value

    async def incr_term(self):
        self.term += 1
        return self.term

    async def get_voted_for(self) -> Union[str, None]:
        return self.voted_for
    
    async def set_voted_for(self, value: str):
        self.voted_for = value

    async def get_last_index(self):
        return self.last_index

    async def get_first_index(self):
        return self.first_index
    
    async def get_last_term(self):
        return self.last_term
    
    async def get_commit_index(self):
        return self.max_commit

    async def get_applied_index(self):
        return self.max_apply

    async def append(self, record: LogRec) -> None:
        save_rec = LogRec.from_dict(record.__dict__)
        self.insert_entry(save_rec)
        return_rec = LogRec.from_dict(save_rec.__dict__)
        logger.debug("new log record %s", return_rec.index)
        return return_rec
    
    async def replace(self, entry:LogRec) -> LogRec:
        if entry.index is None:
            raise Exception("api usage error, call append for new record")
        if entry.index < 1:
            raise Exception("api usage error, cannot insert at index less than 1")
        save_rec = LogRec.from_dict(entry.__dict__)
        self.insert_entry(save_rec)
        return LogRec.from_dict(save_rec.__dict__)

    async def mark_committed(self, index:int) -> None:
        self.max_commit = max(index, self.max_commit)

    async def mark_applied(self, index:int) -> None:
        self.max_apply = max(index, self.max_apply)
    
    async def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if index is None:
            rec = self.get_last_entry()
        else:
            if index < 1:
                raise Exception(f"cannot get index {index}, 1 is the first index")
            if index > self.last_index:
                return None
            if self.first_index is None:
                return None
            if index < self.first_index:
                return None
            rec = self.get_entry_at(index)
        if rec is None:
            return None
        if rec.index <= self.max_commit:
            rec.committed = True
        if rec.index <= self.max_apply:
            rec.applied = True
        return LogRec(**rec.__dict__)
    
    async def delete_all_from(self, index: int):
        if index == 0 or self.snapshot and index == self.snapshot.index + 1:
            self.entries = {}
            self.first_entry = 0
            self.first_index = None
            if self.snapshot:
                self.last_index = self.snapshot.index
                self.last_term = self.snapshot.term
            else:
                self.last_index = 0
                self.last_term = 0
        else:
            keys = list(self.entries.keys())
            keys.sort()
            for rindex in keys[::-1]:
                if rindex >= index:
                    del self.entries[rindex]
            keys = list(self.entries.keys())
            keys.sort()
            if len(keys):
                self.first_index = keys[0]
                self.last_index = keys[-1]
                rec = self.entries[self.last_index]
                self.last_term = rec.term
            else:
                self.first_index = None
                self.last_index = 0
                self.term = 0
                
    async def save_cluster_config(self, config: ClusterConfig) -> None:
        self.nodes = deepcopy(config.nodes)
        self.pending_node = deepcopy(config.pending_node)
        self.cluster_settings = deepcopy(config.settings)
        return ClusterConfig(nodes=deepcopy(self.nodes),
                             pending_node=deepcopy(self.pending_node),
                             settings=deepcopy(self.cluster_settings))
    
    async def get_cluster_config(self):
        if self.nodes is None:
            return None
        return ClusterConfig(nodes=deepcopy(self.nodes),
                             pending_node=deepcopy(self.pending_node),
                             settings=deepcopy(self.cluster_settings))
    
    async def install_snapshot(self, snapshot:SnapShot):
        self.snapshot = snapshot
        end_index = self.snapshot.index
        await self.delete_ending_with(end_index)
        self.last_term = max(self.snapshot.term, self.last_term)
        self.last_index = max(self.snapshot.index, self.last_index)
        if self.last_index > self.snapshot.index:
            self.first_index = self.snapshot.index + 1
        else:
            self.first_index = None
            
    async def get_snapshot(self):
        return self.snapshot
        
    async def get_stats(self) -> LogStats:
        """Get statistics for MemoryLog."""
        # Calculate record count
        record_count = len(self.entries)
        
        # Calculate records since snapshot
        snapshot_index = self.snapshot.index if self.snapshot else 0
        records_since_snapshot = max(0, self.last_index - snapshot_index)
        
        # Calculate records per minute from timestamps
        records_per_minute = 0.0
        if len(self.record_timestamps) >= 2:
            # Use timestamps from last 5 minutes or all available
            current_time = time.time()
            five_minutes_ago = current_time - 300  # 5 minutes
            recent_timestamps = [t for t in self.record_timestamps if t >= five_minutes_ago]
            
            if len(recent_timestamps) >= 2:
                time_span = recent_timestamps[-1] - recent_timestamps[0]
                if time_span > 0:
                    records_per_minute = (len(recent_timestamps) - 1) * 60.0 / time_span
        
        # Memory storage is unlimited
        percent_remaining = None
        
        # Calculate approximate memory usage
        # Rough estimate: each entry ~200 bytes (varies with command length)
        avg_entry_size = 200
        total_size_bytes = record_count * avg_entry_size
        
        return LogStats(
            first_index=await self.get_first_index(),
            last_index=await self.get_last_index(),
            last_term=await self.get_last_term(),
            record_count=record_count,
            records_since_snapshot=records_since_snapshot,
            records_per_minute=records_per_minute,
            percent_remaining=percent_remaining,  # Unlimited storage
            total_size_bytes=total_size_bytes,
            snapshot_index=snapshot_index if snapshot_index > 0 else None,
            last_record_timestamp=self.last_record_time
        )
    
