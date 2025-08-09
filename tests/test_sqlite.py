#!/usr/bin/env python
import logging
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from sqlite_log import SqliteLog
from common import (inner_log_test_basic, inner_log_perf_run,
                    inner_log_test_deletes, inner_log_test_snapshots,
                    inner_log_test_configs
                    )

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

async def test_sqlite_deletes():
    await inner_log_test_deletes(log_create, log_close_and_reopen)


async def test_sqlite_snapshots():
    await inner_log_test_snapshots(log_create, log_close_and_reopen)

async def test_sqlite_configs():
    await inner_log_test_configs(log_create, log_close_and_reopen)
    
async def test_sqlite_specific():
    log = await log_create()
    await log.start()
    stats = await log.get_stats()
    assert stats.percent_remaining is None
