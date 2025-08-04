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
PUSH_SIZE=1000

@dataclass
class PushBlock:
    start_index: int
    end_index: int
    commit_index: int
    apply_index: int
    pushtime: float
    
class HybridLog(LogAPI):
    
    def __init__(self, dirpath, high_limit=5000, low_limit=100):
        self.dirpath = dirpath 
        self.low_limit = low_limit # don't go below this so that we don't get confusing boundary conditions
        self.high_limit = high_limit # time to push to sqlite writer process
        self.sqlite_db_file = Path(dirpath, 'combi_log.db')
        self.sqlite_log = SqliteLog(self.sqlite_db_file, enable_wal=True)
        self.lmdb_db_path = Path(dirpath, 'combi_log.lmdb')
        self.lmdb_log = LmdbLog(self.lmdb_db_path, map_size=LMDB_MAP_SIZE)
        self.last_lmdb_snap = None # this will be snapshot to sqlite, not "real" one
        self.sqlwriter = SqliteWriterControl(self, self.sqlite_db_file, self.lmdb_db_path)
        self.push_blocks = {} # indexed by push time
        self.pending_blocks = []
        self.pending_writes = []
        self.running = True

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
        self.pending_writes.append(rec.index)
        if len(self.pending_writes) >= self.high_limit:
            await self.push_pending()
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

    async def push_pending(self):
        if len(self.push_blocks) > 0:
            lasttime = next(reversed(self.push_blocks))
            lastblock = self.push_blocks[lasttime]
            next_push_count = self.pending_writes[-1] - lastblock.end_index
            start_index = lastblock.end_index + 1
            end_index = start_index + min(next_push_count, PUSH_SIZE)
        else:
            start_index = self.pending_writes[0]
            end_index = self.pending_writes[-1]
        end_index -= self.low_limit
        block = PushBlock(start_index, end_index,
                          commit_index = await self.lmdb_log.get_commit_index(),
                          apply_index = await self.lmdb_log.get_applied_index(),
                          pushtime = time.time())
        self.push_blocks[block.pushtime] = block
        self.pending_blocks.append(block)
        offset = end_index - self.pending_writes[0]
        self.pending_writes = self.pending_writes[offset:]
        snapshot = await self.sqlwriter.send_push(block)

        # Trim pending_writes optimistically (final trim on completion)
        offset = end_index - self.pending_writes[0] + 1  # +1 to include end_index
        self.pending_writes = self.pending_writes[offset:]

    async def handle_snapshot(self, snapshot):
        try:
            block = self.pending_blocks.pop(0)  # FIFO: Process oldest block
            await self.sqlite_log.refresh_stats()
            last_index = await self.lmdb_log.get_last_index()
            first_index = await self.lmdb_log.get_first_index()
            logger.debug(f"before installing sqlite snapshot {snapshot}, " \
                         "last_index = {last_index}, first_index = {first_index}")
            await self.lmdb_log.install_snapshot(snapshot)
            last_index = await self.lmdb_log.get_last_index()
            first_index = await self.lmdb_log.get_first_index()
            logger.debug(f"after installing sqlite snap {snapshot}, "\
                         "last_index = {last_index}, first_index = {first_index}")
            self.last_lmdb_snap = snapshot
            # Final trim: Ensure pending_writes up to block.end_index are removed (in case of races)
            while self.pending_writes and self.pending_writes[0] <= block.end_index:
                self.pending_writes.pop(0)
        except:
            logger.error(f"sqlwriter snashot {snapshot} caused error {traceback.format_exc()}")
            await self.stop()

    async def handle_writer_error(self, error):
        logger.error(f"sqlwriter got error {error}")
        await self.stop()
