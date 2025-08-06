"""
Theory of ops for using Sqlite as an automatic prune assistant for LMDB primary store by
using the Sqlite log as an online, live archive of the primary store. These mechanisms
are the exact, unmodified versions of the Raftengine LogAPI compliant components that
can be used directly.

Notes:
1. The purpose of this design is to reduce the chance that you will run out of space for
   the lmdb records and to do that at a slight performance cost on a fully loaded system.
2. It is unlikely to have the desired effect if a system is loaded to the maximum possible
   record generation rate for its entire service life, as the sqlite store will never catch up.
3. For best results it may need tuning so various dials and knobs are provided.

The incomming records are written to LMDB log immediately. When certain threasholds
are met the SqliteWriter begins copying record to the sqlite log. The LMDB operations
take place in the main process, and the SqliteWriter operations take place in a
child process. Under certain conditions the LMDB side sends a record index as a "limit"
to the SqliteWriter. The SqliteWriter examines its own records and determines
the gap between the its own max index and the sent limit and copies the records from LMDB
log to Sqlite log. It does this by opening a new connection the LMDB log and
keeps it open for the duration of the copy operation, which is performed in loops where
the loop opens a transaction, does some ops, then closes the transaction, thus limiting
the time that it holds a reader lock on the lmdb data. Testing demonstrated that it is
necessary to close and reopen the LMDB log rather than simply keeping an open connection.

Periodically, the SqlWriter process sends a SnapShot to the main process. This is just
like a Raftenine SnapShot record, with the index and the term being those of the targeted
record. This informs the main process that it should install the snapshot, which leads
to pruning the records from the beginning of the LMDB log up to the snapshot index. 
The rate of snapshot operations is a tunable parameter meant to ensure that the main process
does not spend too much time in the record delete operation, which has to be done one at a
time in LMDB. There is a queue of uninstalled snapshots in memory in the LMDB side
to avoid missing things on overlap and to provide an opportunity to throttle the delete operations.

When the user code (actually either the Leader or Follower class) requests a read operation
the HybridLog read code checks to see which log contains the record and routes the request
there. Most other LogAPI operations are performed by simply calling the LDMB log. There are
some exceptions. The install_snapshot and get_snapshot calls are seriviced using the sqlite
log. There is some complexity here when a snapshot index value is in the LMDB log instead
of sqlite, explained below.

The sqlitewriter makes no adjustments to the rate at which it works, it simply copies records until it
catches up to the push limit, a number that will keep growing until it has caught up. Catching up will
only happen if the rate of incomming messages in the lmdb side drops below the maximum rate of the sqlite
side, plus a bit for extra operations. In my testing in a raft environment a typical ratio of rates is
lmdb did about 3800 records per second and sqlite did about 2100. 

Communications between the two is by async socket calls so that both sides can handle the comms
in a fully async fashion. Python multiprocessing is nice to use except for the fact that the
interprocess communications apis are all synchronous, so using them from async code requires methods
that waste time an impact performance near full system load.

The timing of these archiving operations are controlled by these parameters:

1. hold_count - This is the number of records that the LDMB log should retain and not
   send to archive.
2. push_trigger -  This is combined with the calculate value for "pressure" to decide
   when to send a new limit message to the SqliteWriter process.
3. pressure - calculated fron the number of records in LMDB, the hold_count value, and
   the history of the last value of limit sent to the SqliteWriter
4. push_snap_size - This is controls the counter used by the SqliteWriter to decide when
   to send a snapshot to the main process.
5. copy_block_size - This is controls the number of copy operations that the SqliteWriter
   copy loop completes within a single transaction.

"""
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

logger = logging.getLogger('HybridLog')


LMDB_MAP_SIZE=10**9 * 2

class HybridLog(LogAPI):
    
    def __init__(self, dirpath, hold_count=100000, push_trigger=100, push_snap_size=500, copy_block_size=100):
        self.dirpath = dirpath 
        self.hold_count = hold_count
        self.last_pressure_sent = 0
        self.push_trigger = push_trigger
        self.push_snap_size = push_snap_size
        self.copy_block_size = copy_block_size
        self.sqlite_db_file = Path(dirpath, 'combi_log.db')
        self.sqlite_log = SqliteLog(self.sqlite_db_file, enable_wal=True)
        self.lmdb_db_path = Path(dirpath, 'combi_log.lmdb')
        self.lmdb_log = LmdbLog(self.lmdb_db_path, map_size=LMDB_MAP_SIZE)
        self.last_lmdb_snap = None # this will be snapshot to sqlite, not "real" one
        self.sqlwriter = SqliteWriterControl(self.sqlite_db_file, self.lmdb_db_path, snap_size=self.push_snap_size,
                                             copy_block_size=self.copy_block_size)
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
