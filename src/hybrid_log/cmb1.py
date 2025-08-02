import abc
import time
import sys
from dataclasses import dataclass, field, asdict
from typing import Union, List, Optional
import logging
from pathlib import Path
from copy import deepcopy
from raftengine.api.log_api import LogRec, LogAPI, LogStats
from raftengine.api.snapshot_api import SnapShot
from raftengine.api.types import ClusterConfig, NodeRec, ClusterSettings

from sqlite_log.sqlite_log import SqliteLog
from lmdb_log.lmdb_log import LmdbLog

logger = logging.getLogger(__name__)

class CombiLog(LogAPI):
    
    def __init__(self, dirpath):
        self.dirpath = dirpath 
        self.sqlite_db_file = Path(dirpath, 'combi_log.db')
        self.sqlite_log = SqliteLog(self.sqlite_db_file)
        self.lmdb_db_path = Path(dirpath, 'combi_log.lmdb')
        self.lmdb_log = LmdbLog(self.lmdb_db_path)
        self.prefer_lmdb = False

    async def set_lmdb_prefered(self, value):
        self.prefer_lmdb = value
        
    # BEGIN API METHODS
    async def start(self):
        await self.lmdb_log.start()
        await self.sqlite_log.start()

    async def stop(self):
        if self.lmdb_log:
            await self.lmdb_log.stop()
            self.lmdb_log = None
        if self.sqlite_log:
            await self.sqlite_log.stop()
            self.sqlite_log = None

    async def get_broken(self) -> bool:
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_broken()
        return await self.lmdb_log.get_broken()
    
    async def set_broken(self) -> None:
        await self.sqlite_log.set_broken()
        return await self.lmdb_log.set_broken()
    
    async def set_fixed(self) -> None:
        await self.sqlite_log.set_fixed()
        return await self.lmdb_log.set_fixed()

    async def get_term(self) -> Union[int, None]:
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_term()
        return await self.lmdb_log.get_term()
    
    async def set_term(self, value: int):
        await self.sqlite_log.set_term(value)
        return await self.lmdb_log.set_term(value)

    async def incr_term(self):
        return await self.set_term(await self.get_term() + 1)

    async def get_voted_for(self) -> Union[str, None]:
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_voted_for()
        return await self.lmdb_log.get_voted_for()
    
    async def set_voted_for(self, value: str):
        await self.sqlite_log.set_voted_for(value)
        return await self.lmdb_log.set_voted_for(value)

    async def get_last_index(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_last_index()
        return await self.lmdb_log.get_last_index()

    async def get_first_index(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_first_index()
        return await self.lmdb_log.get_first_index()
    
    async def get_last_term(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_last_term()
        return await self.lmdb_log.get_last_term()
    
    async def get_commit_index(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_commit_index()
        return await self.lmdb_log.get_commit_index()

    async def get_applied_index(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_applied_index()
        return await self.lmdb_log.get_applied_index()

    async def append(self, record: LogRec) -> None:
        await self.sqlite_log.append(record)
        return await self.lmdb_log.append(record)
    
    async def replace(self, entry:LogRec) -> LogRec:
        await self.sqlite_log.replace(entry)
        return await self.lmdb_log.replace(entry)

    async def mark_committed(self, index:int) -> None:
        await self.sqlite_log.mark_committed(index)
        return await self.lmdb_log.mark_committed(index)

    async def mark_applied(self, index:int) -> None:
        await self.sqlite_log.mark_applied(index)
        return await self.lmdb_log.mark_applied(index)
    
    async def read(self, index: Union[int, None] = None) -> Union[LogRec, None]:
        if not self.prefer_lmdb:
            return await self.sqlite_log.read(index)
        return await self.lmdb_log.read(index)
    
    async def delete_all_from(self, index: int):
        await self.sqlite_log.delete_all_from(index)
        return await self.lmdb_log.delete_all_from(index)

    async def save_cluster_config(self, config: ClusterConfig) -> None:
        await self.sqlite_log.save_cluster_config(config)
        return await self.lmdb_log.save_cluster_config(config)
    
    async def get_cluster_config(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_cluster_config()
        return await self.lmdb_log.get_cluster_config()
    
    async def install_snapshot(self, snapshot:SnapShot):
        await self.sqlite_log.install_snapshot(snapshot)
        return await self.lmdb_log.install_snapshot(snapshot)
            
    async def get_snapshot(self):
        if not self.prefer_lmdb:
            return await self.sqlite_log.get_snapshot()
        return await self.lmdb_log.get_snapshot()

    async def get_stats(self, from_lmdb=False) -> LogStats:
        if from_lmdb:
            return await self.lmdb_log.get_stats()
        return await self.sqlite_log.get_stats()
    
