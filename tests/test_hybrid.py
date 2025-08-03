#!/usr/bin/env python
import logging
import shutil
from pathlib import Path
import time

import pytest

from raftengine.api.log_api import LogRec
from raftengine.api.log_api import LogRec
from hybrid_log.cmb1 import CombiLog
from common import (inner_log_test_basic, inner_log_perf_run,
                    inner_log_test_deletes, inner_log_test_snapshots,
                    inner_log_test_configs
                    )

async def log_create(instance_number=0):
    path = Path('/tmp', f"test_log_{instance_number}")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    log = CombiLog(path, high_limit=1000, low_limit=100)
    return log

async def log_close_and_reopen(log):
    await log.stop()
    path = Path(log.dirpath)
    log = CombiLog(path)
    return log

async def test_combi_basic():
    await inner_log_test_basic(log_create, log_close_and_reopen)

async def test_combi_deletes():
    await inner_log_test_deletes(log_create, log_close_and_reopen)

async def test_combi_snapshots():
    await inner_log_test_snapshots(log_create, log_close_and_reopen)

async def test_combi_configs():
    await inner_log_test_configs(log_create, log_close_and_reopen)
    
async def test_combi_specific():
    log = await log_create()
    await log.start()
    try:
        await log.set_term(1)
        stats = await log.get_stats()
        for loopc in range(5):
            for index in range(log.high_limit):
                new_rec = LogRec(command=f"add {index}", serial=index)
                rec = await log.append(new_rec)
                last_rec = rec
                assert await log.get_first_index() is not None
                boundary = index
                index += 1
                new_rec = LogRec(command=f"add {index}", serial=index)
                final_rec = await log.append(new_rec)
                # lmdb log will soon have a snapshot
            start_time = time.time()
            while time.time() - start_time < 0.5:
                snap = await log.lmdb_log.get_snapshot()
                if snap:
                    break
                assert snap is not None
                assert await log.get_snapshot() is None # but the facade should say none because not in sqlite
                assert await log.lmdb_log.get_first_index() == snap.index + 1
                assert await log.get_last_index() == final_rec.index
                assert await log.sqlite_log.get_first_index() == 1
                assert await log.sqlite_log.get_last_index() == snap.index

            rec_penult = await log.read(last_rec.index - log.low_limit - 2) # should come from sqlite
            assert last_rec.index - log.low_limit - 2 < await log.get_first_index()
            rec_last = await log.read(last_rec.index - log.low_limit) # should come from lmdb
    finally:
        await log.stop()
