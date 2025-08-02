#!/usr/bin/env python
import logging
import shutil
import tempfile
from copy import deepcopy
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from memory.memory_log import MemoryLog
from common import (inner_log_test_basic, inner_log_perf_run,
                    inner_log_test_deletes, inner_log_test_snapshots,
                    inner_log_test_configs
                    )

async def log_create(instance_number=0):
    return MemoryLog()

async def log_close_and_reopen(log):
    new_log = deepcopy(log)
    await log.stop()
    return new_log

async def test_memory_basic():
    await inner_log_test_basic(log_create, log_close_and_reopen)

async def test_memory_deletes():
    await inner_log_test_deletes(log_create, log_close_and_reopen)

async def test_memory_snapshots():
    await inner_log_test_snapshots(log_create, log_close_and_reopen)

async def test_memory_configs():
    await inner_log_test_configs(log_create, log_close_and_reopen)

async def test_memory_specific():
    log = await log_create()
    await log.start()
    stats = await log.get_stats()
    assert stats.percent_remaining is None

    for i in range(log.max_timestamps):
        rec = LogRec(command=f"add {i}", serial=i)
        await log.append(rec)
    assert len(log.record_timestamps) == log.max_timestamps
    for i in range(1000, 1010):
        rec = LogRec(command=f"add {i}", serial=i)
        await log.append(rec)
    assert len(log.record_timestamps) == log.max_timestamps
        
