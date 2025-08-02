#!/usr/bin/env python
import logging
import shutil
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from raftengine.api.log_api import LogRec
from hybrid_log.cmb1 import CombiLog
from common import (inner_log_test_basic, inner_log_perf_run,
                    inner_log_test_deletes, inner_log_test_snapshots,
                    inner_log_test_configs
                    )

PREFER_LMDB = False
async def log_create(instance_number=0):
    path = Path('/tmp', f"test_log_{instance_number}")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    log = CombiLog(path)
    await log.set_lmdb_prefered(PREFER_LMDB)
    return log

async def log_close_and_reopen(log):
    await log.stop()
    path = Path(log.dirpath)
    log = CombiLog(path)
    await log.set_lmdb_prefered(PREFER_LMDB)
    return log

async def test_combi_basic():
    global PREFER_LMDB
    PREFER_LMDB = False
    await inner_log_test_basic(log_create, log_close_and_reopen)
    PREFER_LMDB = True
    await inner_log_test_basic(log_create, log_close_and_reopen)

async def test_combi_deletes():
    global PREFER_LMDB
    PREFER_LMDB = False
    await inner_log_test_deletes(log_create, log_close_and_reopen)
    PREFER_LMDB = True
    await inner_log_test_deletes(log_create, log_close_and_reopen)

async def test_combi_snapshots():
    global PREFER_LMDB
    PREFER_LMDB = False
    await inner_log_test_snapshots(log_create, log_close_and_reopen)
    PREFER_LMDB = True
    await inner_log_test_snapshots(log_create, log_close_and_reopen)

async def test_combi_configs():
    global PREFER_LMDB
    PREFER_LMDB = False
    await inner_log_test_configs(log_create, log_close_and_reopen)
    PREFER_LMDB = True
    await inner_log_test_configs(log_create, log_close_and_reopen)
    
async def test_combi_specific():
    log = await log_create()
    await log.start()
    stats = await log.get_stats(from_lmdb=True)
    assert stats.percent_remaining is not None
    stats2 = await log.get_stats(from_lmdb=False)
    assert stats2.percent_remaining is None

    await log.set_lmdb_prefered(False)
    await log.set_term(1)
    orig = {}
    for i in range(20):
        rec = LogRec(command=f"add {i}", serial=i)
        rec = await log.append(rec)
        orig[rec.index] = rec
    await log.set_lmdb_prefered(False)
    for i in orig:
        rr = await log.read(i)
        assert rr.command  == orig[i].command
    await log.set_lmdb_prefered(True)
    for i in orig:
        rr = await log.read(i)
        assert rr.command  == orig[i].command

    
