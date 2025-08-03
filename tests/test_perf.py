#!/usr/bin/env python
import os
from statistics import mean, stdev

import pytest

from common import inner_log_perf_run
from test_lmdb import log_create as lmdb_log_create
from test_memlog import log_create as memory_log_create
from test_sqlite import log_create as sqlite_log_create
from test_hybrid import log_create as hybrid_log_create


async def test_lmdb_perf():
    loop_count = int(os.environ.get("RAFTLOG_PERF_LOOPS", 100))
    print('\nMEMORY')
    memory_times = await inner_log_perf_run(memory_log_create, loop_count)
    for key in memory_times:
        k_mean = mean(memory_times[key])
        k_stdev = stdev(memory_times[key])
        total = sum(memory_times[key])
        print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')
    print('\nLMDB')
    lmdb_times = await inner_log_perf_run(lmdb_log_create, loop_count)
    for key in lmdb_times:
        k_mean = mean(lmdb_times[key])
        k_stdev = stdev(lmdb_times[key])
        total = sum(lmdb_times[key])
        print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')
    print('\nHYBRID')
    hybrid_times = await inner_log_perf_run(hybrid_log_create, loop_count)
    for key in hybrid_times:
        k_mean = mean(hybrid_times[key])
        k_stdev = stdev(hybrid_times[key])
        total = sum(hybrid_times[key])
        print(f'{key:10s} total={total:.6f} mean={k_mean:.6f} stdev={k_stdev:.6f}')
    print('\nSQLITE')
    sqlite_times = await inner_log_perf_run(sqlite_log_create, loop_count)
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
    h_p_loop = sum(hybrid_times['loop'])
    hper_max = m_p_loop/h_p_loop
    hcost = h_p_loop/m_p_loop
    h_t_l_cost = h_p_loop/l_p_loop
    s_p_loop = sum(sqlite_times['loop'])
    sper_max = m_p_loop/s_p_loop
    scost = s_p_loop/m_p_loop
    s_t_h_cost = s_p_loop/h_p_loop

    print(f"\n{'Type':12s} ")
    print(f"{'Memory':12s} = {m_p_loop:.6f} Memory x {1.0:>10.6f}")
    print(f"{'LMDB':12s} = {l_p_loop:.6f} Memory x {lcost:>10.6f}")
    print(f"{'HYBRID':12s} = {h_p_loop:.6f} Memory x {hcost:>10.6f} -- LMDB   * {h_t_l_cost:>10.6f}")
    print(f"{'Sqlite':12s} = {s_p_loop:.6f} Memory x {scost:>10.6f} -- Hybrid * {s_t_h_cost:>10.6f}")
