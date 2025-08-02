#!/usr/bin/env python
import logging
import shutil
import tempfile
from copy import deepcopy
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from memory.memory_log import MemoryLog
from common import inner_log_test_basic

async def log_create(instance_number=0):
    return MemoryLog()

async def log_close_and_reopen(log):
    new_log = deepcopy(log)
    await log.stop()
    return new_log

    
async def test_memory_basic():
    await inner_log_test_basic(log_create, log_close_and_reopen)


