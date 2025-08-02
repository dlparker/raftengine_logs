#!/usr/bin/env python
import time

import pytest

from raftengine.api.log_api import LogRec
from raftengine.api.types import ClusterConfig, NodeRec, ClusterSettings
from raftengine.api.snapshot_api import SnapShot

async def inner_log_test_basic(log_create, log_close_and_reopen):
    log = None
    new_log = None
    
    log = await log_create()
    await log.start()

    assert await log.get_last_index() == 0
    assert await log.get_first_index() is None
    assert await log.get_last_term() == 0
    assert await log.get_commit_index() == 0
    assert await log.get_term() == 0
    assert await log.read() is None
    await log.set_term(1)
    rec_1 = LogRec(term=1, command="add 1")
    res_rec_1 = await log.append(rec_1)
    rec_2 = LogRec(term=1, command="add 2")
    res_rec_2 = await log.append(rec_2)
    assert await log.get_first_index() == 1
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
        last_rec = rec

        if i == 19:
            assert await log.get_last_term() == 5
        else:
            assert await log.get_last_term() == 2

    
    assert last_rec.index == 19
    assert await log.get_last_index() == 19
    await log.delete_all_from(4)
    assert await log.read(19) is None
    assert await log.get_last_index() == 3
    assert await log.get_term() == 5
    last_rec = await log.read()
    assert last_rec.index == 3

    # now change something about it and call replace
    last_rec.command = "foobar"
    await log.replace(last_rec)
    last_rec_re_read = await log.read(last_rec.index)
    assert last_rec_re_read.command == "foobar"
    
    
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

    await log.set_voted_for('uri:1')
    await log.set_broken()
    assert await log.get_broken() 
    stats = await log.get_stats()
    assert stats.record_count == 3
    assert stats.records_since_snapshot == 3
    assert stats.records_per_minute > 1
    assert stats.total_size_bytes > 1
    assert stats.snapshot_index is None
    assert stats.last_record_timestamp < time.time()
    
    reopened_log = await log_close_and_reopen(log)
    await reopened_log.start()
    loc_c = await reopened_log.get_commit_index()
    assert loc_c == 2
    rec = await reopened_log.read(2)
    assert rec.index == 2
    assert rec.command == "add 2"
    assert await reopened_log.get_last_index() == 3
    assert await reopened_log.get_term() == 5
    assert await reopened_log.get_applied_index() == 1
    assert await reopened_log.get_commit_index() == 2
    assert await reopened_log.get_broken() 
    await reopened_log.set_fixed()
    assert not await reopened_log.get_broken() 

    assert await reopened_log.get_voted_for() == 'uri:1'

    await reopened_log.mark_committed(last_rec.index)
    assert await reopened_log.get_commit_index() == last_rec.index
    lrr = await reopened_log.read(last_rec.index)
    assert lrr.committed
    assert not lrr.applied
    xx = await reopened_log.read(1)
    assert lrr.committed
    assert not lrr.applied
    await reopened_log.mark_applied(last_rec.index)
    assert await reopened_log.get_applied_index() == last_rec.index
    lrr = await reopened_log.read(last_rec.index)
    assert lrr.applied
    xx = await reopened_log.read(1)
    assert xx.applied
    
    await reopened_log.stop()

async def inner_log_test_deletes(log_create, log_close_and_reopen):
    
    log = await log_create()
    await log.start()
    await log.set_term(1)
    next_i = 1
    while next_i < 11:
        rec = LogRec(command=f"add {next_i}", serial=next_i)
        new_rec = await log.append(rec)
        assert new_rec.index == next_i
        next_i += 1
    assert await log.get_last_index() == 10

    # simulate overwrite when leadership changes with uncommitted records in log
    await log.delete_all_from(9)
    assert await log.get_last_index() == 8

    # simulate overwrite when leadership changes 
    # and all the log entries are uncommitted
    await log.delete_all_from(0)
    x = await log.get_last_index() 
    assert await log.get_last_index() == 0

    await log.stop()

async def inner_log_test_snapshots(log_create, log_close_and_reopen):
    
    log = await log_create()
    await log.start()
    await log.set_term(1)
    next_i = 1
    while next_i < 101:
        rec = LogRec(command=f"add {next_i}", serial=next_i)
        new_rec = await log.append(rec)
        assert new_rec.index == next_i
        next_i += 1

    pre_snap_index = await log.get_last_index()
    assert pre_snap_index == 100
    snapshot = SnapShot(pre_snap_index - 50, 1)
    await log.install_snapshot(snapshot)
    rs = await log.get_snapshot()
    assert rs.index == snapshot.index
    assert rs.term == snapshot.term
    assert await log.get_last_index() == pre_snap_index
    assert await log.get_first_index() == snapshot.index + 1
    stats = await log.get_stats()
    assert stats.record_count == pre_snap_index - 50
    assert stats.records_since_snapshot == 50
    assert stats.records_per_minute > 1
    assert stats.total_size_bytes > 1
    assert stats.snapshot_index == snapshot.index
    assert stats.last_record_timestamp < time.time()

    # install another and make sure everything updates
    snapshot2 = SnapShot(pre_snap_index - 40, 1)
    await log.install_snapshot(snapshot2)
    rs2 = await log.get_snapshot()
    assert rs2.index == snapshot2.index
    assert rs2.term == snapshot2.term
    assert await log.get_last_index() == pre_snap_index
    assert await log.get_first_index() == snapshot2.index + 1
    stats = await log.get_stats()
    assert stats.record_count == pre_snap_index - (pre_snap_index - 40)
    assert stats.records_since_snapshot == 40
    assert stats.snapshot_index == snapshot2.index
    assert await log.read(1) is None

    # snap all the way to the end
    await log.set_term(2)
    snapshot3 = SnapShot(pre_snap_index, 2)
    await log.install_snapshot(snapshot3)
    rs3 = await log.get_snapshot()
    assert rs3.index == snapshot3.index
    assert rs3.term == snapshot3.term
    assert await log.get_last_index() == pre_snap_index
    assert await log.get_first_index() is None
    assert await log.read(1) is None
    assert await log.get_last_term() == 2 # should be read from shapshot term
    
    rec = LogRec(command=f"add {pre_snap_index + 1}", serial=pre_snap_index)
    await log.append(rec)
    assert await log.get_last_index() == pre_snap_index + 1
    assert await log.get_first_index() == pre_snap_index + 1
    

    await log.delete_all_from(await log.get_first_index())
    assert await log.get_first_index() is None
    assert await log.get_last_index() == snapshot3.index


    reopened_log = await log_close_and_reopen(log)
    await reopened_log.start()
    restored_snap = await reopened_log.get_snapshot()
    assert restored_snap.index == snapshot3.index
    assert restored_snap.term == snapshot3.term

    assert await reopened_log.get_first_index() is None
    assert await reopened_log.get_last_index() == snapshot3.index

    
    await log.stop()

async def inner_log_test_configs(log_create, log_close_and_reopen):
    
    log = await log_create()
    await log.start()
    assert await log.get_cluster_config() is None
    settings = ClusterSettings()
    nodes = {}
    n1 = NodeRec(uri='uri:1')
    nodes[n1.uri] = n1
    n2 = NodeRec(uri='uri:2')
    nodes[n2.uri] = n2
    n3 = NodeRec(uri='uri:3')
    nodes[n3.uri] = n3
    cc1 = ClusterConfig(nodes=nodes, settings=settings)
    await log.save_cluster_config(cc1)
    cc2 = await log.get_cluster_config()
    assert cc1.settings == cc2.settings
    assert cc1.nodes == cc2.nodes
    
async def inner_log_perf_run(log_create, loops=1000):
    
    log = await log_create()
    await log.start()

    assert await log.get_last_index() == 0
    assert await log.get_last_term() == 0
    assert await log.get_commit_index() == 0
    assert await log.get_term() == 0
    assert await log.read() is None
    
    await log.set_term(1)
    loop_limit = loops
    loop_count = 0

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
        rec = LogRec(command=f"add {loop_count}", serial=loop_count)
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
        
    return times
        

