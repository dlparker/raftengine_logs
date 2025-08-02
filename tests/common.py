#!/usr/bin/env python
import logging
import shutil
import tempfile
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec

async def inner_log_test_basic(log_create, log_close_and_reopen):
    temp_dir = None  # Track temporary directory for cleanup
    log = None
    new_log = None
    
    try:
        log = await log_create()
        await log.start()

        assert await log.get_last_index() == 0
        assert await log.get_last_term() == 0
        assert await log.get_commit_index() == 0
        assert await log.get_term() == 0
        assert await log.read() is None
        await log.set_term(1)
        rec_1 = LogRec(term=1, command="add 1")
        res_rec_1 = await log.append(rec_1)
        rec_2 = LogRec(term=1, command="add 2")
        res_rec_2 = await log.append(rec_2)
        assert await log.get_last_index() == 2
        assert await log.get_last_term() == 1
        assert res_rec_1.index == 1
        assert res_rec_1.command == 'add 1'
        assert not res_rec_1.committed
        assert not res_rec_1.applied
        assert not res_rec_1.committed

        assert res_rec_2.index == 2
        assert res_rec_2.command == 'add 2'
        assert not res_rec_2.committed
        assert not res_rec_2.applied
        
        rec_2b = await log.read(await log.get_last_index())
        for key in res_rec_2.__dict__:
            assert res_rec_2.__dict__[key] == rec_2b.__dict__[key]
        assert rec_2b.index == 2
        assert rec_2b.command == 'add 2'
        
        await log.mark_committed(res_rec_2.index)
        assert await log.get_commit_index() == 2
        await log.mark_applied(res_rec_1.index)
        assert await log.get_applied_index() == 1
        
        await log.incr_term()
        assert await log.get_term() == 2
        for i in range(3, 20):
            if i == 19:
                await log.set_term(5)
            rec = LogRec(term=await log.get_term(), command=f"add {i}", serial=i)
            rec = await log.append(rec)

            if i == 19:
                assert await log.get_last_term() == 5
            else:
                assert await log.get_last_term() == 2
        assert rec.index == 19
        assert await log.get_last_index() == 19
        await log.delete_all_from(4)
        assert await log.read(19) is None
        assert await log.get_last_index() == 3
        assert await log.get_term() == 5
        last_rec = await log.read()
        assert last_rec.index == 3
        with pytest.raises(Exception):
            await log.read(-3)
        bad_rec = LogRec(index=None, term=await log.get_term(), command=f"add -1", serial=0)
        with pytest.raises(Exception):
            await log.replace(bad_rec)
        bad_rec.index = -1
        with pytest.raises(Exception):
            await log.replace(bad_rec)
        bad_rec.index = 100
        with pytest.raises(Exception):
            await log.replace(bad_rec)
                
        new_log = await log_close_and_reopen(log)
        await new_log.start()
        loc_c = await new_log.get_commit_index()
        assert loc_c == 2
        rec = await new_log.read(2)
        assert rec.index == 2
        assert rec.command == "add 2"
        assert await new_log.get_last_index() == 3
        assert await new_log.get_term() == 5
        assert await new_log.get_applied_index() == 1
        assert await new_log.get_commit_index() == 2
        log = new_log
        new_log = None  # Transfer ownership

        import os
        if 'RAFTLOG_PERF_TEST' in os.environ:
            from statistics import mean, stdev
            import time
            loop_limit = int(os.environ['RAFTLOG_PERF_TEST'])
            loop_count = 0
            print("\\n\\n----------------------------\\n\\n")
            print(f"STARTING PERF TEST {loop_limit} LOOPS")

            times = {
                'loop': [],
                'append': [],
                'read': [],
                'commit': [],
                'apply': [],
            }

            while loop_count < loop_limit:
                # the command cycle is:
                #   1. create log record
                #   2. read it back
                #   3. commit it
                #   4. apply it
                loop_start = time.perf_counter()
                rec = LogRec(term=await log.get_term(), command=f"add {loop_count}", serial=loop_count)
                append_start = time.perf_counter()
                save_rec = await log.append(rec)
                append_end = time.perf_counter()
                read_start = time.perf_counter()
                read_rec = await log.read(save_rec.index)
                read_end = time.perf_counter()
                commit_start = time.perf_counter()
                await log.mark_committed(read_rec.index)
                commit_end = time.perf_counter()
                apply_start = time.perf_counter()
                await log.mark_applied(read_rec.index)
                apply_end = time.perf_counter()
                assert await log.get_commit_index() == read_rec.index
                assert await log.get_applied_index() == read_rec.index
                loop_end = time.perf_counter()
                times['loop'].append(loop_end-loop_start)
                times['append'].append(append_end-append_start)
                times['read'].append(read_end-read_start)
                times['commit'].append(commit_end-commit_start)
                times['apply'].append(apply_end-apply_start)
                loop_count += 1

            for key in times:
                k_mean = mean(times[key])
                k_stdev = stdev(times[key])
                total = sum(times[key])
                print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')

    finally:
        # Proper cleanup
        if new_log:
            try:
                await new_log.stop()
            except:
                pass
        if log:
            try:
                await log.stop()
            except:
                pass
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
            except:
                pass


