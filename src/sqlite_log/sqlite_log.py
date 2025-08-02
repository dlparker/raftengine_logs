import abc
import os
import sqlite3
import time
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
import logging
from raftengine.api.log_api import LogRec, LogAPI, RecordCode, LogStats
from raftengine.api.snapshot_api import SnapShot
from raftengine.api.types import ClusterConfig, NodeRec, ClusterSettings

def bool_converter(value):
    return bool(int(value))

class Records:

    def __init__(self, filepath: os.PathLike):
        # log record indexes start at 1, per raftengine spec
        self.filepath = Path(filepath).resolve()
        self.index = 0
        self.entries = []
        self.db = None
        self.term = -1
        self.broken = None
        self.voted_for = None
        self.max_index = None
        self.max_commit = None
        self.max_apply = None
        self.snapshot = None
        # Don't call open from here, we may be in the wrong thread,
        # at least in testing. Maybe in real server if threading is used.
        # Let it get called when the running server is trying to use it.

    def is_open(self):
        return self.db is not None
    
    def open(self) -> None:
        sqlite3.register_converter('BOOLEAN', bool_converter)
        self.db = sqlite3.connect(self.filepath,
                                  detect_types=sqlite3.PARSE_DECLTYPES |
                                  sqlite3.PARSE_COLNAMES)
        self.db.row_factory = sqlite3.Row
        self.ensure_tables()
        cursor = self.db.cursor()
        sql = "select * from stats"
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
            self.max_index = row['max_index']
            self.term = row['term']
            self.voted_for = row['voted_for']
            self.broken = row['broken']
            self.max_commit = row['max_commit']
            self.max_apply = row['max_apply']
        else:
            self.max_index = 0
            self.term = 0
            self.voted_for = None
            self.broken = False
            self.max_commit = 0
            self.max_apply = 0
            sql = "replace into stats (dummy, max_index, term, voted_for, broken, max_commit, max_apply)" \
                " values (?,?,?,?,?,?,?)"
            cursor.execute(sql, [1, 0, 0, None, False, 0, 0])
        sql = "select * from snapshot where snap_id == 1"
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
           self.snapshot = SnapShot(index=row['s_index'], term=row['term'])
        
    def close(self) -> None:
        if self.db is None:  
            return  # pragma: no cover
        self.db.close()
        self.db = None

    def ensure_tables(self):
        cursor = self.db.cursor()
        schema =  "CREATE TABLE if not exists records " 
        schema += "(rec_index INTEGER PRIMARY KEY AUTOINCREMENT, " 
        schema += "code TEXT, " 
        schema += "command TEXT, " 
        schema += "term int, "
        schema += "leader_id TEXT, "
        schema += "serial TEXT, "
        schema += "timestamp REAL)"
        cursor.execute(schema)
        
        schema = f"CREATE TABLE if not exists stats " \
            "(dummy INTERGER primary key, max_index INTEGER,"\
            " term INTEGER, " \
            " voted_for TEXT, "\
            " max_commit INTEGER," \
            " max_apply INTEGER," \
            " broken BOOLEAN)"
        cursor.execute(schema)

        schema =  "CREATE TABLE if not exists nodes " 
        schema += "(uri TEXT PRIMARY KEY UNIQUE, " 
        schema += "is_adding BOOLEAN, " 
        schema += "is_removing BOOLEAN)" 
        cursor.execute(schema)

        schema =  "CREATE TABLE if not exists settings " 
        schema += "(the_index INTEGER PRIMARY KEY UNIQUE, " 
        schema += "heartbeat_period FLOAT, " 
        schema += "election_timeout_min FLOAT, " 
        schema += "election_timeout_max FLOAT, " 
        schema += "max_entries_per_message int, "
        schema += "use_pre_vote BOOLEAN, "
        schema += "use_check_quorum BOOLEAN, "
        schema += "use_dynamic_config BOOLEAN, "
        schema += "commands_idempotent BOOLEAN) "
        cursor.execute(schema)

        schema = f"CREATE TABLE if not exists snapshot " \
            "(snap_id INTERGER primary key,"\
            " s_index INTERGER NULL, term INTEGER NULL)" 
        cursor.execute(schema)

        self.db.commit()
        cursor.close()
                     
    def save_entry(self, entry):
        if self.db is None: 
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        params = []
        values = "("
        if (entry.index is None and self.snapshot
            and self.snapshot.index == self.max_index):
            # first insert after snapshot, have to fix index
            entry.index = self.max_index + 1
        if entry.index is not None:
            params.append(entry.index)
            sql = f"replace into records (rec_index, "
            values += "?,"
        else:
            sql = f"insert into records ("

        sql += "code, command, term, serial, leader_id, timestamp) values "
        values += "?,?,?,?,?,?)"
        sql += values
        params.append(entry.code)
        params.append(entry.command)
        params.append(entry.term)
        if entry.serial:
            params.append(str(entry.serial))
        else:
            params.append(entry.serial)
        params.append(entry.leader_id)
        params.append(time.time())  # Add current timestamp
        cursor.execute(sql, params)
        entry.index = cursor.lastrowid
        if entry.index > self.max_index:
            self.max_index = entry.index
        sql = "replace into stats (dummy, max_index, term, voted_for, broken, max_commit, max_apply) values (?,?,?,?,?,?,?)"
        cursor.execute(sql, [1, self.max_index, self.term, self.voted_for, self.broken, self.max_commit, self.max_apply])
        self.db.commit()
        cursor.close()
        return self.read_entry(entry.index)

    def read_entry(self, index=None):
        if self.db is None: 
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        if index == None:
            cursor.execute("select max(rec_index) from records")
            row = cursor.fetchone()
            index = row[0]
        sql = "select * from records where rec_index = ?"
        cursor.execute(sql, [index,])
        rec_data = cursor.fetchone()
        if rec_data is None:
            cursor.close()
            return None
        conv = dict(rec_data)
        conv['index'] = rec_data['rec_index']
        del conv['rec_index']
        if rec_data['serial']:
            conv['serial'] = int(rec_data['serial'])
        del conv['timestamp']
        log_rec = LogRec.from_dict(conv)
        cursor.close()
        if self.max_commit >= log_rec.index:
            log_rec.committed = True
        if self.max_apply >= log_rec.index:
            log_rec.applied = True
        return log_rec
    
    def get_broken(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        return self.broken
    
    def set_broken(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        self.broken = True
        self.save_stats()

    def set_fixed(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        self.broken = False
        self.save_stats()

    def save_stats(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        sql = "replace into stats (dummy, max_index, term, voted_for, broken, max_commit, max_apply)" \
            " values (?,?,?,?,?,?,?)"
        cursor.execute(sql, [1, self.max_index, self.term, self.voted_for, self.broken, self.max_commit, self.max_apply])
        self.db.commit()
        cursor.close()
    
    def set_term(self, value):
        self.term = value
        self.save_stats()
    
    def set_voted_for(self, value):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        self.voted_for = value
        self.save_stats()
    
    def get_entry_at(self, index):
        if index < 1:
            return None
        return self.read_entry(index)

    def add_entry(self, rec: LogRec) -> LogRec:
        orig_index = rec.index
        rec.index = None
        rec = self.save_entry(rec)
        return rec

    def insert_entry(self, rec: LogRec) -> LogRec:
        rec = self.save_entry(rec)
        return rec

    def set_commit_index(self, index):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        if index > self.max_commit:
            self.max_commit = index
            self.save_stats()

    def set_apply_index(self, index):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        if index > self.max_apply:
            self.max_apply = index
            self.save_stats()

    def delete_all_from(self, index: int):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        cursor.execute("delete from records where rec_index >= ?", [index,])
        self.max_index = index - 1 if index > 0 else 0
        # If we are deleting committed records that violates raft rules,
        # but this is not the place to enforce that
        self.max_commit = min(self.max_index, self.max_commit)
        self.max_apply = min(self.max_index, self.max_apply)
        sql = "replace into stats (dummy, max_index, term, voted_for, broken, max_commit, max_apply)" \
            " values (?,?,?,?,?,?,?)"
        cursor.execute(sql, [1, self.max_index, self.term, self.voted_for, self.broken, self.max_commit, self.max_apply])
        self.db.commit()
        cursor.close()
    
    def save_cluster_config(self, config: ClusterConfig) -> None:
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        for node in config.nodes.values():
            sql = "insert or replace into nodes (uri, is_adding, is_removing)"
            sql += " values (?,?,?)"
            cursor.execute(sql, [node.uri, node.is_adding, node.is_removing])
        sql = "insert or replace into settings (the_index, heartbeat_period, election_timeout_min,"
        sql += "election_timeout_max, max_entries_per_message, use_pre_vote, "
        sql += " use_check_quorum, use_dynamic_config, commands_idempotent)"
        sql += " values (?,?,?,?,?,?,?,?,?)"
        cs = config.settings
        cursor.execute(sql, [1, cs.heartbeat_period, cs.election_timeout_min, cs.election_timeout_max,
                             cs.max_entries_per_message, cs.use_pre_vote, cs.use_check_quorum,
                             cs.use_dynamic_config, cs.commands_idempotent])
        self.db.commit()
        cursor.close()
    
    def get_cluster_config(self) -> Optional[ClusterConfig]:
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        sql = "select * from settings where the_index == 1"
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            return None
        settings = ClusterSettings(heartbeat_period=row['heartbeat_period'],
                                   election_timeout_min=row['election_timeout_min'],
                                   election_timeout_max=row['election_timeout_max'],
                                   max_entries_per_message=row['max_entries_per_message'],
                                   use_pre_vote=row['use_pre_vote'],
                                   use_check_quorum=row['use_check_quorum'],
                                   use_dynamic_config=row['use_dynamic_config'],
                                   commands_idempotent=row['commands_idempotent'])
        nodes = {}
        sql = "select * from nodes"
        cursor.execute(sql)
        for row in cursor.fetchall():
            rec = NodeRec(uri=row['uri'],
                          is_adding=row['is_adding'],
                          is_removing=row['is_removing'])
            nodes[rec.uri] = rec
        res = ClusterConfig(nodes=nodes, settings=settings)
        return res

    def get_first_index(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        if self.snapshot:
            if self.max_index > self.snapshot.index:
                return self.snapshot.index + 1
            return None
        if self.max_index > 0:
            return 1
        return None

    def get_last_index(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        if not self.snapshot:
            return self.max_index
        return max(self.max_index, self.snapshot.index)
    
    async def install_snapshot(self, snapshot):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        cursor = self.db.cursor()
        sql = "delete from snapshot"
        cursor.execute(sql)
        sql = "insert into snapshot (snap_id, s_index, term) values (?,?,?)"
        cursor.execute(sql, [1, snapshot.index, snapshot.term])
        sql = "delete from records where rec_index <= ?"
        cursor.execute(sql, [snapshot.index,])
        cursor.close()
        self.db.commit()
        self.max_index = max(snapshot.index, self.max_index)
        self.snapshot = snapshot

    def get_snapshot(self):
        if self.db is None: # pragma: no cover
            self.open() # pragma: no cover
        return self.snapshot

class SqliteLog(LogAPI):

    def __init__(self, filepath: os.PathLike):
        self.records = None
        self.filepath = filepath
        self.logger = logging.getLogger(__name__)

    async def start(self):
        # this indirection helps deal with the need to restrict
        # access to a single thread
        self.records = Records(self.filepath)
        if not self.records.is_open(): # pragma: no cover
            self.records.open() # pragma: no cover
        
    async def stop(self):
        self.records.close()
        
    async def get_broken(self):
        return self.records.get_broken()
    
    async def set_broken(self):
        return self.records.set_broken()

    async def set_fixed(self):
        return self.records.set_fixed()

    async def get_term(self) -> Union[int, None]:
        if not self.records.is_open(): # pragma: no cover
            self.records.open() # pragma: no cover
        return self.records.term
    
    async def set_term(self, value: int):
        self.records.set_term(value)

    async def get_voted_for(self) -> Union[int, None]:
        return self.records.voted_for
    
    async def set_voted_for(self, value: str):
        self.records.set_voted_for(value)
        
    async def incr_term(self):
        self.records.set_term(self.records.term + 1)
        return self.records.term

    async def append(self, entry: LogRec) -> None:
        save_rec = LogRec.from_dict(entry.__dict__)
        return_rec = self.records.add_entry(save_rec)
        self.logger.debug("new log record %s", return_rec.index)
        return return_rec

    async def replace(self, entry:LogRec) -> LogRec:
        if entry.index is None:
            raise Exception("api usage error, call append for new record")
        if entry.index < 1:
            raise Exception("api usage error, cannot insert at index less than 1")
        save_rec = LogRec.from_dict(entry.__dict__)
        next_index = self.records.max_index + 1
        if save_rec.index > next_index:
            raise Exception("cannot replace record with index greater than max record index")
        self.records.insert_entry(save_rec)
        return LogRec.from_dict(save_rec.__dict__)

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
        return LogRec.from_dict(rec.__dict__)

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

    async def mark_committed(self, index:int) -> None:
        self.records.set_commit_index(index)
    
    async def mark_applied(self, index:int) -> None:
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
    
    async def get_first_index(self) -> int:
        return self.records.get_first_index()

    async def install_snapshot(self, snapshot:SnapShot):
        return await self.records.install_snapshot(snapshot)

    async def get_snapshot(self) -> Optional[SnapShot]: 
        return self.records.get_snapshot()

    async def get_stats(self) -> LogStats:
        """Get statistics for SqliteLog."""
        if not self.records.is_open(): # pragma: no cover
            self.records.open()
        
        cursor = self.records.db.cursor()
        
        # Get record count
        cursor.execute("SELECT COUNT(*) FROM records")
        record_count = cursor.fetchone()[0]
        
        # Calculate records since snapshot
        snapshot_index = self.records.snapshot.index if self.records.snapshot else 0
        records_since_snapshot = max(0, self.records.max_index - snapshot_index)
        
        # Calculate records per minute from timestamps
        records_per_minute = 0.0
        current_time = time.time()
        five_minutes_ago = current_time - 300  # 5 minutes
        
        # Get timestamps from the last 5 minutes
        cursor.execute("SELECT timestamp FROM records WHERE timestamp >= ? ORDER BY timestamp", 
                      [five_minutes_ago])
        recent_timestamps = [row[0] for row in cursor.fetchall() if row[0] is not None]
        
        if len(recent_timestamps) >= 2:
            time_span = recent_timestamps[-1] - recent_timestamps[0]
            if time_span > 0:
                records_per_minute = (len(recent_timestamps) - 1) * 60.0 / time_span
        
        # Get last record timestamp
        cursor.execute("SELECT MAX(timestamp) FROM records")
        last_timestamp_result = cursor.fetchone()[0]
        last_record_timestamp = last_timestamp_result if last_timestamp_result else None
        
        total_size_bytes = os.path.getsize(self.records.filepath)
        
        cursor.close()
        
        return LogStats(
            record_count=record_count,
            records_since_snapshot=records_since_snapshot,
            records_per_minute=records_per_minute,
            percent_remaining=None,  # SQLite has unlimited storage
            total_size_bytes=total_size_bytes,
            snapshot_index=snapshot_index if snapshot_index > 0 else None,
            last_record_timestamp=last_record_timestamp
        )

