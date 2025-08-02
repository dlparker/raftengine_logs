#!/usr/bin/env python
import logging
import shutil
import tempfile
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from memory.memory_log import MemoryLog
from sqlite.sqlite_log import SqliteLog
from lmdb1.lmdb_log import LmdbLog
from common import inner_log_testfunc

async def test_memory_log():
    await inner_log_testfunc(log_type=MemoryLog)

async def test_sqlite_log():
    await inner_log_testfunc(log_type=SqliteLog)

async def test_lmdb_log():
    await inner_log_testfunc(log_type=LmdbLog)

