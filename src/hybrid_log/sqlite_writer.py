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

    def __init__(self, sqlite_db_path, lmdb_db_path):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path
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
            self.writer_task = asyncio.create_task(writer_task(self.sqlite_db_path, self.lmdb_db_path, self.port))
            await asyncio.sleep(0.01)
            logger.warning("sqlwriter Task started, test and debug only!!!")
        else:
            args = [self.sqlite_db_path, self.lmdb_db_path, self.port]
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
            
    async def send_push(self, block):  # Renamed from push; now non-blocking
        command = dict(command='push_block', block=asdict(block))
        msg_str = json.dumps(command)
        msg_bytes = msg_str.encode()
        count = str(len(msg_bytes))
        self.writer.write(f"{count:20s}".encode())
        self.writer.write(msg_bytes)
        await self.writer.drain()
        
    async def send_quit(self):
        command = dict(command='quit')
        msg_str = json.dumps(command)
        msg_bytes = msg_str.encode()
        count = str(len(msg_bytes))
        self.writer.write(f"{count:20s}".encode())
        self.writer.write(msg_bytes)
        await self.writer.drain()
        
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
                    logger.debug('handling reply snapshot %s', str(snap))
                    asyncio.create_task(self.callback(snap))
                    await asyncio.sleep(0.001)
                except Exception as e:
                    logger.error('get_snaps got error \n%s', traceback.format_exc())
                    asyncio.create_task(self.error_callback(traceback.format_exc()))
                    breakpoint()
            except asyncio.CancelledError:
                logger.warning('get_snaps got cancelled')
                await self.stop()
                break
            

async def writer_runner(sqlite_file_path, lmdb_db_path, port):
    try:
        writer = SqliteWriter(sqlite_file_path, lmdb_db_path, port)
        await writer.start()
        await writer.serve()
    except:
        traceback.print_exc()
    
    
def writer_process(sqlite_file_path, lmdb_db_path, port):
    asyncio.run(writer_runner(sqlite_file_path, lmdb_db_path, port))

async def writer_task(sqlite_file_path, lmdb_db_path, port):
    # This is just for testing support, having it run
    # in process helps with debugging and coverage.
    # It is not appropriate for actual use as you'll
    # lose all the benifit of the hybrid approach
    await writer_runner(sqlite_file_path, lmdb_db_path, port)

class SqliteWriterService:

    def __init__(self, sqlwriter, port):
        self.sqlwriter = sqlwriter
        self.port = port
        self.sock_server = None
        self.server_task = None
        self.shutdown_event = asyncio.Event()
        self.running = False

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
                try:
                    await self.sock_server.wait_closed()
                    self.sock_server = None
                except asyncio.CancelledError:
                    pass
                finally:
                    self.sock_server = None
        logger.info("SqliteWriterService exiting")
        self.server_task = None
        self.running = False
        
    async def stop(self):
        if self.running:
            self.shutdown_event.set()
            await asyncio.sleep(0.01)
            self.running = False
            self.sock_server = None

    async def handle_client(self, reader, writer):
        # intended to handle exactly one connection, more is improper usage
        logger.debug("SqliteWriterService connection from %s", writer.get_extra_info("peername"))
        while self.running:
            try:
                logger.debug("SqliteWriterService reading for length")
                len_data = await reader.read(20)
                logger.debug("SqliteWriterService got length data %s", len_data)
                if not len_data:
                    logger.debug("SqliteWriterService got None on read")
                    await self.stop()
                    break
                msg_len = int(len_data.decode().strip())
                logger.debug("SqliteWriterService got length msg_len %d", msg_len)
                
                # Read message data
                data = await reader.read(msg_len)
                logger.debug("SqliteWriterService got msg data %s", data)
                if not data:
                    logger.debug("SqliteWriterService got None on read")
                    await self.stop()
                    break
                try:
                    request = json.loads(data.decode())
                except:
                    logger.error(f'JSON failure on data {data.decode()}')
                    break
                try:
                    logger.debug("SqliteWriterService got request %s", request)
                    if request['command'] == 'quit':
                        logger.info("quitting on command")
                        await self.stop()
                        break
                    if request['command'] == 'push_block':
                        res = await self.sqlwriter.push_block(request['block'])
                        result = json.dumps(res, default=lambda o: o.__dict__)
                        response = result.encode()
                        count = str(len(response))
                        writer.write(f"{count:20s}".encode())
                        writer.write(response)
                        await writer.drain()
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
        writer.close()
        reader.close()
        logger.warning("SqliteWriterService handler exiting")
    
class SqliteWriter:

    def __init__(self, sqlite_db_path, lmdb_db_path, port):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path        
        self.port = port
        self.writer_service = SqliteWriterService(self, port=self.port)
        from hybrid_log.hybrid_log import LMDB_MAP_SIZE
        from hybrid_log.hybrid_log import PUSH_SIZE
        
        self.sqlite = SqliteLog(self.sqlite_db_path, enable_wal=True)
        self.lmdb = LmdbLog(self.lmdb_db_path, map_size=LMDB_MAP_SIZE)
        self.started = False

    async def start(self):
        if not self.started:
            await self.sqlite.start()
            await self.lmdb.start()
            await self.writer_service.start()
            self.started = True

    async def serve(self):
        await self.writer_service.serve()
        
    async def push_block(self, block_dict):
        from hybrid_log.hybrid_log import PushBlock
        try:
            block = PushBlock(**block_dict)
            logger.debug(f'handling block {block}')
            snapshot = await self.do_push(block)
            logger.debug(f'posting reply snapshot {snapshot}')
            return {'result': {'index': snapshot.index, 'term': snapshot.term}, 'error': None}
        except Exception as e:
            logger.error(traceback.format_exc())
            return {'error': str(e), 'result': None}
        
    async def do_push(self, block):
        if self.lmdb.records is not None:  # Avoid error on first call
            await self.lmdb.stop()
            await self.lmdb.start()
        try:
            s_last_index = await self.lmdb.get_last_index()
            s_first_index = await self.lmdb.get_first_index()
            logger.debug("starting copy %d-%d last_index=%d, first_index=%d",
                         block.start_index, block.end_index, s_last_index, s_first_index)
            for index in range(block.start_index, block.end_index + 1):
                rec = await self.lmdb.read(index)
                if rec is None:
                    raise Exception(f'\n\nNone read at index {index}, sli = {s_last_index} sfi = {s_first_index}\n\n')
                await self.sqlite.append(rec)
            # update local stats from new record efects
            max_index = await self.sqlite.get_last_index()
            max_term = await self.sqlite.get_last_term()
            await self.sqlite.set_term(max_term)
            logger.debug("Block.commit_index = %d", block.commit_index)
            await self.sqlite.mark_committed(min(max_index, block.commit_index))
            await self.sqlite.mark_applied(min(max_index, block.apply_index))
            logger.debug("local.commit_index = %d", await self.sqlite.get_commit_index())
            snapshot = SnapShot(max_index, max_term)
            return snapshot
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e
        
    
