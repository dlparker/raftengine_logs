"""
Theory of ops for using Sqlite as an automatic prune assistant for LMDB primary store.

The incomming records are written to LMDB storage immediately. When certain threasholds
are met the Sqlite portion, the SqliteWriter begins copying record to sqlite storage.
The LMDB operations take place in the main process, and the SqliteWriter operaions
take place in a child process. Under certain conditions the LMDB side sends the current
maximum record index (kinda, details below) to the SqliteWrite. The SqliteWriter examines
its own records and determines the gap between the two and copies the records from LMDB
store to Sqlite store. It does this by opening a new connection the LMDB store and
keeps it open for the duration of the copy operation.

When the user code (actually either the Leader or Follower class) requests a read operation
the HybridLog read code checks to see which store contains the record and routes the request
there. Most other LogAPI operations are performed by simply calling the LDMB log. There are
some exceptions. The install_snapshot and get_snapshot calls are seriviced using the sqlite
store. There is some complexity here when a snapshot index value is in the LMDB store instead
of sqlite, explained below.

Periodically as the SqliteWriter copy operation runs, the current maximum index in Sqlite is sent
to the LMDB side as a snapshot record. This format is used because the LMDB store support
snapshot based pruning as is required for the Raftengine LogAPI, so no additional code is
required in order to perform this task. The rate of snapshot operations is a tunable parameter
meant to ensure that the main process does not spend too much time in the record delete
operation, which has to be done one at a time in LMDB. There is a queue of uninstalled
snapshots in memory in the LMDB side to avoid missing things on overlap and to provide
an opportunity to throttle the delete operations.


1. The purpose of this design is to reduce the chance that you will run out of space for
   the lmdb records and to do that at a slight performance cost on a fully loaded system.
2. It is unlikely to have the desired effect if a system is loaded to the maximum possible
   record generation rate for its entire service life, as the sqlite store will never catch up.
3. For best results it may need tuning so various dials and knobs are provided.

LMDB side: Pressure = record count - low_limit == records to send. If this is more than 1000, send the
index value of the record at low limit which is just max_index-low_limit. Send this to SqliteWriter.
SqliteWriter updates its value for copy_stop to this value. If the copy task is not runnning, it is
started.

The copy task does a loop on min(loop_limit, copy_stop-max_index). After copying the records,
it checks the last_snapp_index value and if it is gte the snap threashold it sends a snapshot.

When the snapshot arrives at the LMDB side it is queued in the snapshot queue and the snapshot
installer task is started if not already running.


Values:
1. Record limit - an estimate of the number of records that the lmdb storage can hold. This value
   is not likely to reach its best accuracy until there a thousands of records present.
2. Max record count - the largest number of records that have been present in lmdb storage
3. Average record count - average count of lmdb records measured every time 1000 new records are
   added. Thus this value reflects the current state of the store, more or less.
4. LT Average record count - the average number of lmdb records over the total life of the store.
5. Low limit - the threashold at which pushing to sqlite stops, when lmdb record count meets this number.
6. Push trigger - the threashold at which pushing to sqlite starts, when lmdb record count meets this number.
7. Push limit - sqlite writer side value that indicates where the writer should stop copying.
8. Push Block Size - How many records will be copied per copy loop. Each loop completion will trigger
   a snapshot push to the main process.

The sqlitewriter makes no adjustments to the rate at which it works, it simply copies records until it
catches up to the push limit, a number that will keep growing until it has caught up. Catching up will
only happen if the rate of incomming messages in the lmdb side drops below the maximum rate of the sqlite
side, plus a bit for extra operations. In my testing in a raft environment a typical ratio of rates is
lmdb did about 3800 records per second and sqlite did about 2100. So a maximally loaded system will never
catch up, but 50 maximum load should keep up pretty well.



When the lmdb record count hits push trigger, a the record index of the currently inserting record is sent to
the sqlite writer. This sets the push



Communications between the two is by async socket
calls so that both sides can handle the comms in a fully async fashion. Python
multiprocessing is nice to use except for the fact that the interprocess communications
apis are all synchronous, so using them from async code requires methods that waste time
an impact performance near full system load.


When using the python-lmdb library, you can open a read-write transaction and create a cursor to traverse the database. For example, you can use mdb_cursor_get() with operations like MDB_FIRST to start at the beginning of the database and MDB_NEXT to move through each record, deleting them one by one with mdb_cursor_del().
 This approach leverages LMDB's efficient memory-mapped design, allowing for zero-copy access to data and minimizing overhead.

Snapshots go to sqlite in all cases. If index is in LMDB, the lmdb records are pruned as needed
but the snapshot is not installed in LDMB since that is reserved for SqliteWriter snapshots.

"""
import asyncio
import time
import sys
import json
from random import randint
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

logger = logging.getLogger('hybrid_log.sqlwriter')


class SqliteWriterControl:

    def __init__(self, sqlite_db_path, lmdb_db_path, snap_size):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path
        self.snap_size = snap_size
        self.record_list = None
        self.port = None
        self.writer_proc = None
        self.reader = None
        self.writer = None
        self.running = False
        self.callback = None
        self.error_callback = None
        self.writer_task = None

    async def start(self, callback, error_callback, port=None, inprocess=False):
        self.callback = callback
        self.error_callback = error_callback
        if port is None:
            port = randint(8000, 65000)
        self.port = port
        if inprocess:
            # This is just for testing support, having it run
            # in process helps with debugging and coverage.
            # It is not appropriate for actual use as you'll
            # lose all the benifit of the hybrid approach
            self.writer_task = asyncio.create_task(writer_task(self.sqlite_db_path, self.lmdb_db_path, self.port, self.snap_size))
            await asyncio.sleep(0.01)
            logger.warning("sqlwriter Task started, test and debug only!!!")
        else:
            args = [self.sqlite_db_path, self.lmdb_db_path, self.port, self.snap_size]
            self.writer_proc = Process(target=writer_process, args=args)
            self.writer_proc.start()
            await asyncio.sleep(0.01)
            logger.debug("sqlwriter process started")
        self.running = True
        self.reader, self.writer = await asyncio.open_connection('localhost', self.port)
        asyncio.create_task(self.get_snaps())

    async def stop(self):
        self.running = False
        if self.writer:
            await self.send_quit()
        if self.writer_proc:
            self.writer_proc.join()
            self.writer_proc = None
        if self.writer_task:
            try:
                self.writer_task.cancel()
            except asyncio.CancelledError:
                pass

    async def send_command(self, command):
        msg_str = json.dumps(command)
        msg_bytes = msg_str.encode()
        count = str(len(msg_bytes))
        self.writer.write(f"{count:20s}".encode())
        self.writer.write(msg_bytes)
        await self.writer.drain()
        
    async def send_limit(self, limit, commit_index, apply_index):  
        command = dict(command='copy_limit', limit=limit, commit_index=commit_index, apply_index=apply_index)
        await self.send_command(command)
        
    async def set_snap_size(self, size):
        self.snap_size = size
        command = dict(command='snap_size', size=size)
        await self.send_command(command)
        
    async def send_quit(self):
        command = dict(command='quit')
        await self.send_command(command)
        
    async def get_snaps(self):
        logger.debug('get_snaps started')
        while self.running:
            try:
                try:
                    len_data = await self.reader.read(20)
                    if not len_data:
                        logger.warning("Connection closed by server")
                        await self.stop()
                        break
                    msg_len = int(len_data.decode().strip())
                    data = await self.reader.read(msg_len)
                    if not data:
                        logger.warning("No data received, connection closed")
                        await self.stop()
                        break
                    # Parse response
                    response = json.loads(data.decode())
                    if not self.running:
                        break
                    if response['error'] is not None:
                        logger.error(f'reply shows serror {res["error"]}')
                        await self.stop()
                        break
                    result = response['result']
                    snap = SnapShot(result['index'], result['term'])
                    logger.debug('\n--------------------\nhandling reply snapshot %s\n----------------------\b', str(snap))
                    asyncio.create_task(self.callback(snap))
                except Exception as e:
                    logger.error('get_snaps got error \n%s', traceback.format_exc())
                    asyncio.create_task(self.error_callback(traceback.format_exc()))
                    breakpoint()
            except asyncio.CancelledError:
                logger.warning('get_snaps got cancelled')
                await self.stop()
                break
            except ConnectionResetError:
                logger.warning('get_snaps got dropped socket')
                await self.stop()
                break
        self.running = False
        
async def writer_runner(sqlite_file_path, lmdb_db_path, port, snap_size):
    try:
        writer = SqliteWriter(sqlite_file_path, lmdb_db_path, port, snap_size)
        await writer.start()
        await writer.serve()
    except:
        traceback.print_exc()
    
    
def writer_process(sqlite_file_path, lmdb_db_path, port, snap_size):
    asyncio.run(writer_runner(sqlite_file_path, lmdb_db_path, port, snap_size))
    logger.debug("writer process exiting")

async def writer_task(sqlite_file_path, lmdb_db_path, port, snap_size):
    # This is just for testing support, having it run
    # in process helps with debugging and coverage.
    # It is not appropriate for actual use as you'll
    # lose all the benifit of the hybrid approach
    await writer_runner(sqlite_file_path, lmdb_db_path, port, snap_size)

class SqliteWriterService:

    def __init__(self, sqlwriter, port):
        self.sqlwriter = sqlwriter
        self.port = port
        self.sock_server = None
        self.server_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False
        self.writer = None
        self.writer = None

    async def start(self):
        self.running = True
        self.sock_server = await asyncio.start_server(
            self.handle_client, '127.0.0.1', self.port
        )
        
    async def serve(self):
        logger.info("SqliteWriterService listening on %d", self.port)
        try:
            # Keep the server running
            await self.shutdown_event.wait()
            logger.debug("serve got shutdown")
        except asyncio.CancelledError:
            # Server is being shut down
            pass
        finally:
            if self.sock_server:
                self.sock_server.close()
                self.sock_server = None
        logger.info("SqliteWriterService exiting")
        self.server_task = None
        self.running = False
        
    async def stop(self):
        self.shutdown_event.set()
                
    async def send_message(self, message):
        wrapper = dict(result=message, error=None)
        msg = json.dumps(wrapper, default=lambda o: o.__dict__)
        msg = msg.encode()
        count = str(len(msg))
        self.writer.write(f"{count:20s}".encode())
        self.writer.write(msg)
        await self.writer.drain()
        
    async def handle_client(self, reader, writer):
        # intended to handle exactly one connection, more is improper usage
        logger.debug("SqliteWriterService connection from %s", writer.get_extra_info("peername"))
        self.writer = writer
        while self.running:
            try:
                #logger.debug("SqliteWriterService reading for length")
                len_data = await reader.read(20)
                #logger.debug("SqliteWriterService got length data %s", len_data)
                if not len_data:
                    logger.debug("SqliteWriterService got None on read")
                    break
                msg_len = int(len_data.decode().strip())
                #logger.debug("SqliteWriterService got length msg_len %d", msg_len)
                
                # Read message data
                data = await reader.read(msg_len)
                #logger.debug("SqliteWriterService got msg data %s", data)
                if not data:
                    logger.debug("SqliteWriterService got None on read")
                    break
                try:
                    request = json.loads(data.decode())
                except:
                    logger.error(f'JSON failure on data {data.decode()}')
                    break
                try:
                    #logger.debug("SqliteWriterService got request %s", request)
                    if request['command'] == 'quit':
                        logger.info("quitting on command")
                        break
                    elif request['command'] == 'snap_size':
                        await self.sqlwriter.set_snap_size(request['size'])
                    elif request['command'] == 'pop_snap':
                        # really just for testing:
                        #logger.debug('looking for snap to pop')
                        if self.sqlwriter.pending_snaps:
                            snapshot = self.sqlwriter.pending_snaps.pop(0)
                            logger.debug('sending popped snap %s', str(snapshot))
                            await self.send_message(snapshot)
                    elif request['command'] == 'copy_limit':
                        res = await self.sqlwriter.set_copy_limit(request)
                        if res:
                            # this will be a snapshot if anything
                            logger.debug('sending %s', str(res))
                            await self.send_message(res)
                    else:
                        # never heard of itn
                        logger.warning(f'Got request of unknown type {request}')
                except:
                    logger.error("Error processing request %s\n%s", request, traceback.format_exc())
                    break
            except asyncio.CancelledError:
                logger.warning("SqliteWriterService handler canceled")
                break
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                logger.error("SqliteWriterService handler error \n%s", traceback.format_exc())
                break
            except Exception:
                logger.error("SqliteWriterService handler error \n%s", traceback.format_exc())
                break
        await self.stop()
        logger.warning("SqliteWriterService handler exiting")
    
class SqliteWriter:

    def __init__(self, sqlite_db_path, lmdb_db_path, port, snap_size):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path        
        self.port = port
        self.snap_size = snap_size
        self.writer_service = SqliteWriterService(self, port=self.port)
        from hybrid_log.hybrid_log import LMDB_MAP_SIZE
        self.sqlite = SqliteLog(self.sqlite_db_path, enable_wal=True)
        self.lmdb = LmdbLog(self.lmdb_db_path, map_size=LMDB_MAP_SIZE)
        self.started = False
        self.last_snap_index = 0
        self.copy_limit = 0
        self.copy_task_handle = None
        self.pending_snaps = []
        self.upstream_commit_index = 0
        self.upstream_apply_index = 0

    async def start(self):
        if not self.started:
            await self.sqlite.start()
            await self.lmdb.start()
            await self.writer_service.start()
            self.started = True

    async def serve(self):
        await self.writer_service.serve()
        logger.debug("Sqlwriter.serve() exited")
        await self.stop()

    async def stop(self):
        if self.copy_task_handle:
            self.copy_task_handle.cancel()
            await self.sqlite.stop()
            await self.lmdb.stop()
        
    async def set_snap_size(self, size):
        self.snap_size = size
        
    async def set_copy_limit(self, props):
        limit = props['limit']
        self.upstream_commit_index = props['commit_index']
        self.upstream_apply_index = props['apply_index']
        self.copy_limit = limit
        if not self.copy_task_handle:
            self.copy_task_handle = asyncio.create_task(self.copy_task())
        if self.pending_snaps:
            return self.pending_snaps.pop(0)
        return None
    
    async def copy_task(self):
        try:
            my_last = await self.sqlite.get_last_index() 
            while my_last < self.copy_limit:
                if self.lmdb.records is not None:  # Avoid error on first call
                    await self.lmdb.stop()
                    await self.lmdb.start()
                limit_index = min(self.copy_limit, my_last + 100)
                recs = []
                logger.debug("SqliteWriterService copy task copying from %d to %d", my_last, limit_index)
                for index in range(my_last, limit_index+1):
                    if index == 0:
                        continue # indices start from 1
                    rec = await self.lmdb.read(index)
                    if rec:
                        recs.append(rec)
                await self.lmdb.stop()
                local_term = await self.sqlite.get_term()
                for rec in recs:
                    if rec.term > local_term:
                        local_term = rec.term
                        await self.sqlite.set_term(local_term)
                    await self.sqlite.append(rec)
                logger.debug("SqliteWriterService copy task finished copying from %d to %d", my_last, limit_index)
                # update local stats from new record efects
                max_index = await self.sqlite.get_last_index()
                max_term = await self.sqlite.get_last_term()
                await self.sqlite.set_term(max_term)
                local_commit = await self.sqlite.get_commit_index()
                local_apply = await self.sqlite.get_applied_index()
                if self.upstream_commit_index > local_commit:
                    max_commit = min(self.upstream_commit_index, max_index)
                    await self.sqlite.mark_committed(max_commit)
                    logger.debug("updated sqlite commit to %d", max_commit)
                if self.upstream_apply_index > local_apply:
                    max_apply = min(self.upstream_apply_index, max_index)
                    await self.sqlite.mark_applied(max_apply)
                    logger.debug("updated sqlite applied to %d", max_apply)
                while max_index - self.last_snap_index >= self.snap_size:
                    snap_index = self.last_snap_index + self.snap_size
                    snap_rec = await self.sqlite.read(snap_index)
                    snapshot = SnapShot(snap_rec.index, snap_rec.term)
                    self.pending_snaps.append(snapshot)
                    self.last_snap_index = snap_rec.index
                    logger.debug('appended pending snapshot %s', str(snapshot))
                my_last = await self.sqlite.get_last_index()
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e

        self.copy_task_handle = None
    
    
