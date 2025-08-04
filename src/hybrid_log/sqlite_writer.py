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

logger = logging.getLogger('hybrid_log.sqlwriter')


class SqliteWriterControl:

    def __init__(self, cmb1, sqlite_db_path, lmdb_db_path):
        self.cmb1 = cmb1
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path
        self.record_list = None
        self.writer_process = None
        self.command_queue = None
        self.reply_queue = None
        self.running = False
        self.callback = None
        self.error_callback = None

    async def start(self, callback, error_callback):
        self.callback = callback
        self.error_callback = error_callback
        self.command_queue = Queue()
        self.reply_queue = Queue()
        args = [self.sqlite_db_path, self.lmdb_db_path, self.command_queue, self.reply_queue]
        self.writer_process = Process(target=writer_wrapper, args=args)
        self.writer_process.start()
        logger.debug(f"sqlwriter process started")
        self.running = True
        asyncio.create_task(self.get_snaps())

    async def stop(self):
        self.running = False
        self.command_queue.put(None)
        self.writer_process.join()

    async def send_push(self, block):  # Renamed from push; now non-blocking
        command = dict(command='push_block', block=asdict(block))
        self.command_queue.put(command)
        
    async def get_snaps(self):
        logger.debug('get_snaps started')
        while self.running:
            try:
                try:
                    res = await asyncio.to_thread(self.reply_queue.get_nowait)
                except queue.Empty:
                    continue
                if not self.running:
                    return
                if res['error'] is not None:
                    logger.error(f'reply shows serror {res["error"]}')
                    raise Exception(f'Queue returned error {res["error"]}')
                result = res['result']
                snap = SnapShot(result['index'], result['term'])
                logger.debug('handling reply snapshot %s', str(snap))
                asyncio.create_task(self.callback(snap))
                await asyncio.sleep(0.001)
            except Exception:
                logger.error('get_snaps got error %s', traceback.format_exc())
                asyncio.create_task(self.error_callback(traceback.format_exc()))
   
def writer_wrapper(sqlite_file_path, lmdb_db_path, command_queue, reply_queue):
    writer = SqliteWriter(sqlite_file_path, lmdb_db_path, command_queue, reply_queue)
    asyncio.run(writer.main_loop())
            
class SqliteWriter:

    def __init__(self, sqlite_db_path, lmdb_db_path, command_queue, reply_queue):
        self.sqlite_db_path = sqlite_db_path
        self.lmdb_db_path = lmdb_db_path        
        self.command_queue = command_queue
        self.reply_queue = reply_queue
        from hybrid_log.hybrid_log import LMDB_MAP_SIZE
        from hybrid_log.hybrid_log import PUSH_SIZE
        
        self.sqlite = SqliteLog(self.sqlite_db_path, enable_wal=True)
        self.lmdb = LmdbLog(self.lmdb_db_path, map_size=LMDB_MAP_SIZE)
        self.started = False
        self.running = False

    async def main_loop(self):
        from hybrid_log.hybrid_log import PushBlock
        self.running = True
        logger.debug(f'sqlwriter main loop started')
        while self.running:
            command = self.command_queue.get()
            try:
                if command is None:
                    break
                if command['command'] == "push_block":
                    block = PushBlock(**command['block'])
                    logger.debug(f'handling block {block}')
                    snapshot = await self.push_block(block)
                    logger.debug(f'posting reply snapshot {snapshot}')
                    self.reply_queue.put({'result': {'index': snapshot.index, 'term': snapshot.term}, 'error': None})
                else:
                    logger.debug(f'posting reply invalid command {command}')
                    self.reply_queue.put({'result': None, 'error': f"Invalid command object {command}"})
            except Exception as e:
                logger.error(traceback.format_exc())
                self.reply_queue.put({'error': str(e), 'resule': None})
        self.running = False
        logger.debug(f'sqlwriter main loop exited')
        
    async def push_block(self, block):
        if self.lmdb.records is not None:  # Avoid error on first call
            await self.lmdb.stop()
            await self.lmdb.start()
        if not self.started:
            await self.sqlite.start()
            await self.lmdb.start()
            self.started = True
        try:
            s_last_index = await self.lmdb.get_last_index()
            s_first_index = await self.lmdb.get_first_index()
            #print(f'\nWriter:starting copy {block.start_index}-{block.end_index} li={s_last_index} fi={s_first_index}\n')
            for index in range(block.start_index, block.end_index + 1):
                rec = await self.lmdb.read(index)
                if rec is None:
                    raise Exception(f'\n\nNone read at index {index}, sli = {s_last_index} sfi = {s_first_index}\n\n')
                await self.sqlite.append(rec)
            # update local stats from new record efects
            max_index = await self.sqlite.get_last_index()
            max_term = await self.sqlite.get_last_term()
            await self.sqlite.set_term(max_term)
            #print(f"Block.commit_index = {block.commit_index}")
            await self.sqlite.mark_committed(min(max_index, block.commit_index))
            await self.sqlite.mark_applied(min(max_index, block.apply_index))
            #print(f"local.commit_index = {await self.sqlite.get_commit_index()}")
            snapshot = SnapShot(max_index, max_term)
            return snapshot
        except Exception as e:
            print(f"\n\n --------------- Exception in writer process ----------\n\n")
            traceback.print_exc()
            print(f"\n\n -------------------------------------------------------\n\n")
            raise
        
