#!/usr/bin/env python
import logging
import shutil
import tempfile
from pathlib import Path

import pytest

from raftengine.api.log_api import LogRec
from lmdb1.lmdb_log import LmdbLog
from common import inner_log_test_basic


async def log_create(instance_number=0):
    path = Path('/tmp', "test_log_{instance_number}.lmdb")
    if path.exists():
        shutil.rmtree(path)
    log = LmdbLog(path)
    return log

async def log_close_and_reopen(log):
    path = Path(log.filepath)
    await log.stop()
    log = LmdbLog(path)
    return log

async def test_lmdb_basic():
    await inner_log_test_basic(log_create, log_close_and_reopen)

