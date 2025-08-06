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
    log = HybridLog(path)
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

async def seq1(log):
    await log.start()

    try:
        await log.set_term(1)
        stats = await log.get_stats()
        for loopc in range(10):
            await log.set_term(loopc+1)
            start_index = await log.get_first_index()
            if start_index is None:
                start_index = 1
            threashold_index = start_index + log.hold_count  + 1
            last_at_loop_start = await log.lmdb_log.get_last_index()
            next_index = last_at_loop_start + 1
            while next_index < threashold_index:
                new_rec = LogRec(command=f"add {next_index}", serial=next_index, term=loopc)
                rec = await log.append(new_rec)
                next_index += 1
            # make sure commit and apply get updated properly in sqlite
            await log.mark_committed(next_index -1)
            await log.mark_applied(next_index -1)
            assert await log.lmdb_log.get_commit_index() == next_index -1
            assert await log.lmdb_log.get_applied_index() == next_index -1
            # now write another to cross threashold
            new_rec = LogRec(command=f"add {next_index}", serial=next_index, term=loopc)
            rec = await log.append(new_rec)
            next_index += 1
            # we won't pick up the snapshot without more records, so
            # let's cheat and trigger it
            start_time = time.time()
            while time.time() - start_time < 1.0:
                await asyncio.sleep(0.001)
                await log.sqlwriter.send_command(dict(command="pop_snap")) 
                if start_index != await log.get_first_index():
                    break

                
            assert start_index != await log.get_first_index()
            snap = await log.lmdb_log.get_snapshot()
            assert snap is not None
            # the snapshot should cover the records from the last snapshot
            # or the start_index end plus the snapshot size 
            #
            calc = start_index + log.push_snap_size - 1 # includes the start index
            assert snap.index == calc
            # make sure commit and apply get updated properly in sqlite
            assert await log.sqlite_log.get_commit_index() == snap.index
            assert await log.sqlite_log.get_applied_index() == snap.index

    finally:
        await log.stop()
    

async def test_hybrid_specific():
    class HL(HybridLog):

        async def start(self):
            await self.lmdb_log.start()
            await self.sqlite_log.start()
            await self.sqlwriter.start(self.handle_snapshot, self.handle_writer_error, inprocess=True)
    path = Path('/tmp', f"test_log_1_ip")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    log1 = HL(path, hold_count=2, push_trigger=1, push_snap_size=2, copy_block_size=2)
    await seq1(log1)
    path = Path('/tmp', f"test_log_1_mp")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    log2 = HybridLog(path, hold_count=2, push_trigger=1, push_snap_size=2, copy_block_size=2)
    await seq1(log2)


