import asyncio
import time
import sys
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
import logging
from dataclasses import dataclass, asdict
from multiprocessing import Manager, Process, Queue
import queue
import traceback
from pathlib import Path
from copy import deepcopy
from raftengine.api.log_api import LogRec, LogAPI, LogStats
from raftengine.api.snapshot_api import SnapShot
from raftengine.api.types import ClusterConfig, NodeRec, ClusterSettings

from sqlite_log.sqlite_log import SqliteLog
from lmdb_log.lmdb_log import LmdbLog
from hybrid_log.sqlite_writer import SqliteWriterControl

logger = logging.getLogger('hybrid_log')


LMDB_MAP_SIZE=10**9 * 2

class HybridLog(LogAPI):
    
    def __init__(self, dirpath, hold_count=100000, push_snap_size=500):
        self.dirpath = dirpath 
        self.hold_count = hold_count
        self.last_pressure_sent = 0
        self.push_trigger = 100
        self.push_snap_size = push_snap_size
        self.sqlite_db_file = Path(dirpath, 'combi_log.db')
        self.sqlite_log = SqliteLog(self.sqlite_db_file, enable_wal=True)
        self.lmdb_db_path = Path(dirpath, 'combi_log.lmdb')
        self.lmdb_log = LmdbLog(self.lmdb_db_path, map_size=LMDB_MAP_SIZE)
        self.last_lmdb_snap = None # this will be snapshot to sqlite, not "real" one
        self.sqlwriter = SqliteWriterControl(self.sqlite_db_file, self.lmdb_db_path, snap_size=self.push_snap_size)
        self.pending_snaps = []
        self.running = True

    def set_hold_count(self, value):
        self.hold_count = value

    async def set_snap_size(self, value):
        self.push_snap_size = value
        await self.sqlwriter.send_snap_size(value)

    # BEGIN API METHODS
    async def start(self):
        await self.lmdb_log.start()
        await self.sqlite_log.start()
        await self.sqlwriter.start(self.handle_snapshot, self.handle_writer_error)
        
    async def stop(self):
        self.running = False
        if self.lmdb_log:
            await self.lmdb_log.stop()
            self.lmdb_log = None
        if self.sqlite_log:
            await self.sqlite_log.stop()
            self.sqlite_log = None
        if self.sqlwriter:
            await self.sqlwriter.stop()
            self.sqlwriter = None
            
    async def get_broken(self) -> bool:
        return  await self.sqlite_log.get_broken() or await self.lmdb_log.get_broken()
    
    async def set_broken(self) -> None:
        await self.sqlite_log.set_broken()
        return await self.lmdb_log.set_broken()
    
    async def set_fixed(self) -> None:
        await self.sqlite_log.set_fixed()
        return await self.lmdb_log.set_fixed()

    async def get_term(self) -> Union[int, None]:
        return await self.lmdb_log.get_term()
    
    async def set_term(self, value: int):
        await self.sqlite_log.set_term(value)
        return await self.lmdb_log.set_term(value)

    async def incr_term(self):
        return await self.set_term(await self.get_term() + 1)

    async def get_voted_for(self) -> Union[str, None]:
        return await self.lmdb_log.get_voted_for()
    
    async def set_voted_for(self, value: str):
        await self.sqlite_log.set_voted_for(value)
        return await self.lmdb_log.set_voted_for(value)

    async def get_last_index(self):
        return await self.lmdb_log.get_last_index()

    async def get_first_index(self):
        lfirst = await self.lmdb_log.get_first_index()
        if lfirst is None:
            return await self.sqlite_log.get_first_index()
        return lfirst
    
    async def get_last_term(self):
        return await self.lmdb_log.get_last_term()
    
    async def get_commit_index(self):
        return await self.lmdb_log.get_commit_index()

    async def get_applied_index(self):
        return await self.lmdb_log.get_applied_index()
        
    async def append(self, record: LogRec) -> None:
        rec = await self.lmdb_log.append(record)
        last = await self.lmdb_log.get_last_index()
        first = await self.lmdb_log.get_first_index()
        if first is None:
            first = 0
        local_count = last - first
        # The last_pressure_sent value is either is or will become
        # the first index. So if we have enough records in local
        # store, taking this into account, to exceed the number
        # we are supposed to retain, then do a signal to
        # SqliteWriter. The number to signal is a record index
        # to be the copy_stop value. That will be the current
        # last index (from the new record) minus the hold count.
        current_pressure = local_count - self.last_pressure_sent - self.hold_count
        if current_pressure >= self.push_trigger:
            await self.sqlwriter.send_limit(rec.index - self.hold_count,
                                            await self.lmdb_log.get_commit_index(),
                                            await self.lmdb_log.get_applied_index())
            await asyncio.sleep(0.0)
        return rec

    async def replace(self, entry:LogRec) -> LogRec:
        rec = await self.lmdb_log.replace(entry)
        await self.sqlite_log.replace(rec)
        return rec

    async def mark_committed(self, index:int) -> None:
        return await self.lmdb_log.mark_committed(index)

    async def mark_applied(self, index:int) -> None:
        return await self.lmdb_log.mark_applied(index)
    
    async def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if index is None:
            return await self.lmdb_log.read(index)
        elif self.last_lmdb_snap and self.last_lmdb_snap.index >= index:
            return await self.sqlite_log.read(index)
        return await self.lmdb_log.read(index)
    
    async def delete_all_from(self, index: int):
        await self.sqlite_log.delete_all_from(index)
        return await self.lmdb_log.delete_all_from(index)

    async def save_cluster_config(self, config: ClusterConfig) -> None:
        await self.sqlite_log.save_cluster_config(config)
        return await self.lmdb_log.save_cluster_config(config)
    
    async def get_cluster_config(self):
        return await self.lmdb_log.get_cluster_config()
    
    async def install_snapshot(self, snapshot:SnapShot):
        await self.sqlite_log.install_snapshot(snapshot)
        lm_snap = await self.lmdb_log.get_snapshot()
        if lm_snap and lm_snap.index > snapshot.index:
            # sqlite write snapshot already cleared records
            # past the end of the new "real" snapshot,
            # so lmdb does not change
            return
        await self.lmdb_log.install_snapshot(snapshot)
        return snapshot
            
    async def get_snapshot(self):
        # always get it from sqlite, that way we can use
        # the one in lmdb to track snapshost to sqlite
        # instead of "real" snapshots
        return await self.sqlite_log.get_snapshot()

    async def get_stats(self, from_lmdb=False) -> LogStats:
        return await self.lmdb_log.get_stats()

    async def handle_snapshot(self, snapshot):
        async def process_snapshot(curr_snapshot):
            try:
                await self.sqlite_log.refresh_stats()
                last_index = await self.lmdb_log.get_last_index()
                first_index = await self.lmdb_log.get_first_index()
                logger.debug(f"before installing sqlite snapshot {curr_snapshot}, " \
                             f"lmdb_last_index = {last_index}, lmdb_first_index = {first_index}")
                await self.lmdb_log.install_snapshot(curr_snapshot)
                last_index = await self.lmdb_log.get_last_index()
                first_index = await self.lmdb_log.get_first_index()
                logger.debug(f"after installing sqlite snap {curr_snapshot}, "\
                             f"lmdb_last_index = {last_index}, lmdb_first_index = {first_index}")
                self.last_lmdb_snap = curr_snapshot
                if self.pending_snaps:
                    next_snapshot = self.pending_snaps.pop(0)
                    await asyncio.sleep(0.001)
                    asyncio.create_task(process_snapshot(next_snapshot))
            except:
                logger.error(f"sqlwriter snashot {snapshot} caused error {traceback.format_exc()}")
                await self.stop()
        self.pending_snaps.append(snapshot)
        logger.debug("got sqlitewriter snapshot %s", str(snapshot))
        next_snapshot = self.pending_snaps.pop(0)
        asyncio.create_task(process_snapshot(next_snapshot))
            

    async def handle_writer_error(self, error):
        logger.error(f"sqlwriter got error {error}")
        await self.stop()
