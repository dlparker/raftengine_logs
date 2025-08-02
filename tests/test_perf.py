#!/usr/bin/env python
from statistics import mean, stdev

import pytest

from common import inner_log_perf_run
from test_lmdb import log_create as lmdb_log_create
from test_memlog import log_create as memory_log_create
from test_sqlite import log_create as sqlite_log_create


async def test_lmdb_perf():
    memory_times = await inner_log_perf_run(memory_log_create)
    print('MEMORY')
    for key in memory_times:
        k_mean = mean(memory_times[key])
        k_stdev = stdev(memory_times[key])
        total = sum(memory_times[key])
        print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')
    lmdb_times = await inner_log_perf_run(lmdb_log_create)
    print('LMDB')
    for key in lmdb_times:
        k_mean = mean(lmdb_times[key])
        k_stdev = stdev(lmdb_times[key])
        total = sum(lmdb_times[key])
        print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')
    lmdb_times = await inner_log_perf_run(lmdb_log_create)
    sqlite_times = await inner_log_perf_run(sqlite_log_create)
    print('SQLITE')
    for key in sqlite_times:
        k_mean = mean(sqlite_times[key])
        k_stdev = stdev(sqlite_times[key])
        total = sum(sqlite_times[key])
        print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')

    m_p_loop = sum(memory_times['loop'])
    mper_max = 1.0
    mcost = 0
    l_p_loop = sum(lmdb_times['loop'])
    lper_max = m_p_loop/l_p_loop
    lcost = l_p_loop/m_p_loop
    s_p_loop = sum(sqlite_times['loop'])
    sper_max = m_p_loop/s_p_loop
    scost = s_p_loop/m_p_loop

    print(f"{'Type':12s}  {'per loop':9s} % of Max")
    print(f"{'Memory':12s} = {m_p_loop:.6f} {mper_max:.6f} Memory x  = {mcost:.6f}")
    print(f"{'LMDB':12s} = {l_p_loop:.6f} {lper_max:.6f} Memory x  = {lcost:.6f}")
    print(f"{'Sqlite':12s} = {s_p_loop:.6f} {sper_max:.6f} Memory x  = {scost:.6f}")
