#!/usr/bin/env python
import asyncio
import logging
import shutil
from pathlib import Path
import time

import pytest
from raftengine.api.log_api import LogRec
from raftengine.api.log_api import LogRec
from hybrid_log.hybrid_log import HybridLog
from common import (inner_log_test_basic, inner_log_perf_run,
                    inner_log_test_deletes, inner_log_test_snapshots,
                    inner_log_test_configs
                    )


async def log_create(instance_number=0):
    path = Path('/tmp', f"test_log_{instance_number}")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    log = HybridLog(path, high_limit=1000, low_limit=100)
    return log

async def log_close_and_reopen(log):
    await log.stop()
    path = Path(log.dirpath)
    log = HybridLog(path)
    return log

async def test_hybrid_basic():
    await inner_log_test_basic(log_create, log_close_and_reopen)

async def test_hybrid_deletes():
    await inner_log_test_deletes(log_create, log_close_and_reopen)

async def test_hybrid_snapshots():
    await inner_log_test_snapshots(log_create, log_close_and_reopen)

async def test_hybrid_configs():
    await inner_log_test_configs(log_create, log_close_and_reopen)
    
async def test_hybrid_specific():
    log = await log_create()
    await log.start()

    # Want to make sure that records flow to sqlite and
    # get pruned from lmdb. The hybrid log keeps track
    # of the record ids that have been written to lmdb
    # but not to sqlite, and when they reach or exceed
    # the high_limit parameter it pushes a block of record ids
    # off to sqlite. It copies those records, and then
    # sends back a snapshot to the main process, where
    # it gets installed into the lmdb log. 
    # So, for each loop, figure out the first record index
    # that might be tracked. If there is no snapshot, then
    # it is the first record in the lmdb. If there is a snapshot,
    # then it is still the first record in the db!
    # Next, calculate how many records need to be added to 
    # hit the limit. This will be the number of high_limit +1
    # passed the last_index value at the start of the loop
    try:
        await log.set_term(1)
        log.high_limit = 100
        log.low_limit = 10
        stats = await log.get_stats()
        # write one record first to bypass annoying first_index = None problem
        new_rec = LogRec(command=f"add {1}", serial=1)
        rec = await log.append(new_rec)
        for loopc in range(10):
            start_index = await log.get_first_index()
            threashold_index = start_index + log.high_limit  - 1
            last_at_loop_start = await log.lmdb_log.get_last_index()
            next_index = last_at_loop_start + 1
            while next_index < threashold_index:
                new_rec = LogRec(command=f"add {next_index}", serial=next_index)
                rec = await log.append(new_rec)
                next_index += 1
            # make sure commit and apply get updated properly in sqlite
            await log.mark_committed(next_index -1)
            await log.mark_applied(next_index -1)
            assert await log.lmdb_log.get_commit_index() == next_index -1
            assert await log.lmdb_log.get_applied_index() == next_index -1
            # now write another to cross threashold
            new_rec = LogRec(command=f"add {next_index}", serial=next_index)
            rec = await log.append(new_rec)
            next_index += 1
            start_time = time.time()
            while time.time() - start_time < 1.0:
                await asyncio.sleep(0.001)
                if start_index != await log.get_first_index():
                    break
            assert start_index != await log.get_first_index()
            snap = await log.lmdb_log.get_snapshot()
            assert snap is not None
            calc = start_index + log.high_limit - log.low_limit
            if start_index == 1:
                calc -= 1 # special case for first loop, one record already saved
            #print(f"{snap.index} == ({start_index} -1 + {log.high_limit} - {log.low_limit}) {calc}")
            assert snap.index == calc
            # make sure commit and apply get updated properly in sqlite
            assert await log.sqlite_log.get_commit_index() == snap.index
            assert await log.sqlite_log.get_applied_index() == snap.index

    finally:
        await log.stop()
