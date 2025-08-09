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

from sqlite_log import SqliteLog
from lmdb_log import LmdbLog

logger = logging.getLogger('HybridLog.sqlite_writer')


class SqliteWriterControl:

    def __init__(self, sqlite_db_path, lmdb_db_path, snap_size, copy_block_size):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path
        self.snap_size = snap_size
        self.copy_block_size = copy_block_size
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
            self.writer_task = asyncio.create_task(
                writer_task(self.sqlite_db_path, self.lmdb_db_path, self.port, self.snap_size, self.copy_block_size))
            await asyncio.sleep(0.01)
            logger.warning("sqlwriter Task started, test and debug only!!!")
        else:
            args = [self.sqlite_db_path, self.lmdb_db_path, self.port, self.snap_size, self.copy_block_size]
            self.writer_proc = Process(target=writer_process, args=args)
            self.writer_proc.start()
            await asyncio.sleep(0.01)
            logger.debug("sqlwriter process started")
        self.running = True
        self.reader, self.writer = await asyncio.open_connection('localhost', self.port)
        asyncio.create_task(self.read_backchannel())

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
        try:
            try:
                self.writer.write(f"{count:20s}".encode())
                self.writer.write(msg_bytes)
                await self.writer.drain()
            except asyncio.CancelledError:
                pass
        except ConnectionResetError:
            logger.warning(traceback.format_exc())
        
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
        
    async def read_backchannel(self):
        logger.debug('read_backchannel started')
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
                    msg_wrapper = json.loads(data.decode())
                    if not self.running:
                        break
                    logger.debug("read_backchannel message %s", msg_wrapper)
                    if msg_wrapper['code'] == "command_error":
                        logger.error(f'reply shows serror {msg_wrapper["message"]}')
                    elif msg_wrapper['code'] == "snapshot":
                        snap = msg_wrapper['message']
                        snapshot = SnapShot(snap['index'], snap['term'])
                        logger.debug('\n--------------------\nhandling reply snapshot %s\n----------------------\b', str(snapshot))
                        asyncio.create_task(self.callback("snapshot", snapshot))
                    elif msg_wrapper['code'] == "stats":
                        stats = msg_wrapper['message']
                        logger.debug('handling reply stats %s', stats)
                        asyncio.create_task(self.callback("stats", stats))
                    else:
                        logger.error('\n--------------------\nhandling reply message code unknown %s\n----------------------\b',
                                     msg_wrapper['code'])
                except Exception as e:
                    logger.error('read_backchannel got error \n%s', traceback.format_exc())
                    asyncio.create_task(self.error_callback(traceback.format_exc()))
                    breakpoint()
            except asyncio.CancelledError:
                logger.warning('read_backchannel got cancelled')
                await self.stop()
                break
            except ConnectionResetError:
                logger.warning('read_backchannel got dropped socket')
                await self.stop()
                break
        self.running = False
        


class SqliteWriter:

    def __init__(self, sqlite_db_path, lmdb_db_path, port, snap_size, copy_block_size):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path        
        self.port = port
        self.snap_size = snap_size
        self.copy_block_size = copy_block_size
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
        # Rate tracking fields for statistics
        self.copy_block_timestamps = []  # For blocks_per_minute calculation
        self.copy_blocks_count = 0

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
    
    async def get_stats(self) -> dict:
        """Get writer-side statistics"""
        import time
        current_time = time.time()
        five_minutes_ago = current_time - 300
        
        # Clean old timestamps and calculate rate
        self.copy_block_timestamps = [ts for ts in self.copy_block_timestamps if ts >= five_minutes_ago]
        copy_blocks_per_minute = len(self.copy_block_timestamps) * 12  # Convert to per-minute
        
        sqlite_commit = await self.sqlite.get_commit_index()
        sqlite_apply = await self.sqlite.get_applied_index()
        
        return {
            'writer_pending_snaps_count': len(self.pending_snaps),
            'current_copy_limit': self.copy_limit,
            'upstream_commit_lag': self.upstream_commit_index - sqlite_commit,
            'upstream_apply_lag': self.upstream_apply_index - sqlite_apply,
            'copy_blocks_per_minute': copy_blocks_per_minute
        }
    
    async def copy_task(self):
        try:
            my_last = await self.sqlite.get_last_index() 
            while my_last < self.copy_limit:
                if self.copy_limit - my_last < self.copy_block_size:
                    await asyncio.sleep(0.001)
                    continue
                if self.lmdb.records is not None:  # Avoid error on first call
                    await self.lmdb.stop()
                    await self.lmdb.start()
                limit_index = my_last + self.copy_block_size
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
                
                # Track copy block rate for statistics
                import time
                self.copy_blocks_count += 1
                self.copy_block_timestamps.append(time.time())
                # Limit list size for memory efficiency
                if len(self.copy_block_timestamps) > 1000:
                    self.copy_block_timestamps = self.copy_block_timestamps[-500:]
                
                my_last = await self.sqlite.get_last_index()
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e

        self.copy_task_handle = None
    

def writer_process(sqlite_file_path, lmdb_db_path, port, snap_size, copy_block_size):
    # this is the main function of the writer process
    asyncio.run(writer_runner(sqlite_file_path, lmdb_db_path, port, snap_size, copy_block_size))
    logger.debug("writer process exiting")

async def writer_task(sqlite_file_path, lmdb_db_path, port, snap_size, copy_block_size):
    # This is just for testing support, having it run
    # in process helps with debugging and coverage.
    # It is not appropriate for actual use as you'll
    # lose all the benifit of the hybrid approach
    await writer_runner(sqlite_file_path, lmdb_db_path, port, snap_size, copy_block_size)

async def writer_runner(sqlite_file_path, lmdb_db_path, port, snap_size, copy_block_size):
    try:
        writer = SqliteWriter(sqlite_file_path, lmdb_db_path, port, snap_size, copy_block_size)
        await writer.start()
        await writer.serve()
    except:
        traceback.print_exc()
        
    
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
                
    async def send_message(self, code, message):
        wrapper = dict(code=code, message=message)
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
                            await self.send_message(code="snapshot", message=snapshot)
                    elif request['command'] == 'copy_limit':
                        res = await self.sqlwriter.set_copy_limit(request)
                        if res:
                            # this will be a snapshot if anything
                            logger.debug('sending %s', str(res))
                            await self.send_message(code="snapshot", message=res)
                    elif request['command'] == 'get_stats':
                        stats = await self.sqlwriter.get_stats()
                        await self.send_message(code="stats", message=stats)
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
    
    
