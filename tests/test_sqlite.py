#!/usr/bin/env python
import logging
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from sqlite.sqlite_log import SqliteLog
from common import inner_log_test_basic

async def log_create(instance_number=0):
    path = Path('/tmp', "test_log_{instance_number}.db")
    if path.exists():
        path.unlink()
    log = SqliteLog(path)
    return log

async def log_close_and_reopen(log):
    await log.stop()
    path = Path(log.records.filepath)
    log = SqliteLog(path)
    return log

async def test_sqlite_basic():
    await inner_log_test_basic(log_create, log_close_and_reopen)

