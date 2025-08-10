#!/usr/bin/env python
import asyncio
import logging
import shutil
from pathlib import Path
import time

import pytest
from raftengine.api.log_api import LogRec
from raftengine.api.log_api import LogRec
from raftengine_logs.hybrid_log import HybridLog
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

class HL(HybridLog):
       
    async def start(self):
        await self.lmdb_log.start()
        await self.sqlite_log.start()
        await self.sqlwriter.start(self.sqlwriter_callback, self.handle_writer_error, inprocess=True)
        
async def seq1(use_in_process=False):

    if use_in_process:
        path = Path('/tmp', f"test_log_1_ip")
    else:
        path = Path('/tmp', f"test_log_seq1")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    
    # Use controlled tuning parameters for predictable behavior
    if use_in_process:
        log = HL(path, hold_count=10, push_trigger=5, push_snap_size=5, copy_block_size=3)
    else:
        log = HybridLog(path, hold_count=10, push_trigger=5, push_snap_size=5, copy_block_size=3)
    await log.start()

    try:
        await log.set_term(1)
        
        # Write records to trigger archiving
        # We need to write enough records so that:
        # local_count - last_pressure_sent - hold_count >= push_trigger
        # Starting with last_pressure_sent = 0, we need local_count >= hold_count + push_trigger = 15
        
        for i in range(1, 20):  # Write 19 records
            new_rec = LogRec(command=f"add {i}", serial=i, term=1)
            rec = await log.append(new_rec)
            await log.mark_committed(i)
            await log.mark_applied(i)
            
            # Check if archiving has been triggered by monitoring first_index change
            if i >= 15:  # After we've written enough to trigger archiving
                await asyncio.sleep(0.1)  # Give time for async processing
                
        # Wait for snapshot processing to complete
        start_time = time.time()
        original_first_index = await log.get_first_index()
        while time.time() - start_time < 2.0:
            await asyncio.sleep(0.05)
            current_first_index = await log.get_first_index()
            if current_first_index != original_first_index:
                break
            # Trigger snapshot processing if needed
            await log.sqlwriter.send_command(dict(command="pop_snap"))
        
        # Verify that archiving occurred
        final_first_index = await log.get_first_index()
        assert final_first_index != original_first_index, f"First index should have changed from {original_first_index}"
        
        # Verify snapshot was created in LMDB
        snap = await log.lmdb_log.get_snapshot()
        assert snap is not None, "Snapshot should exist in LMDB"
        
        # Verify snapshot index is reasonable (should be around push_snap_size records)
        expected_snap_index = original_first_index + log.push_snap_size - 1
        assert snap.index == expected_snap_index, f"Snapshot index {snap.index} should be {expected_snap_index}"
        
        # The key assertion is that archiving occurred - snapshot exists and first_index changed
        print(f"Success: Archiving triggered. First index changed from {original_first_index} to {final_first_index}")
        print(f"Snapshot created with index {snap.index}")
        
        # Note: sqlite_log commit/apply indices may be different from snapshot index
        # as they track different aspects of the system state

    finally:
        await log.stop()
    

async def test_hybrid_specific():
    await seq1(use_in_process=True)
    await seq1()


async def test_enhanced_stats():
    """Test enhanced hybrid log statistics"""
    path = Path('/tmp', f"test_enhanced_stats")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    
    # Use controlled tuning parameters
    log = HybridLog(path, hold_count=8, push_trigger=3, push_snap_size=4, copy_block_size=2)
    await log.start()

    try:
        await log.set_term(1)
        
        # Add records to generate some statistics
        for i in range(1, 15):
            new_rec = LogRec(command=f"stats_test {i}", serial=i, term=1)
            await log.append(new_rec)
            await log.mark_committed(i)
            await log.mark_applied(i)
            await asyncio.sleep(0.05)  # Small delay to allow processing
        
        # Wait for writer processing
        await asyncio.sleep(0.5)
        
        # Get enhanced statistics
        stats = await log.get_hybrid_stats()
        
        # Verify we got HybridStats with expected fields
        assert hasattr(stats, 'lmdb_stats'), "Should have lmdb_stats field"
        assert hasattr(stats, 'sqlite_stats'), "Should have sqlite_stats field"
        assert hasattr(stats, 'ingress_rate'), "Should have ingress_rate field"
        assert hasattr(stats, 'copy_rate'), "Should have copy_rate field"
        assert hasattr(stats, 'copy_lag'), "Should have copy_lag field"
        assert hasattr(stats, 'current_pressure'), "Should have current_pressure field"
        assert hasattr(stats, 'pending_snaps_count'), "Should have pending_snaps_count field"
        assert hasattr(stats, 'writer_pending_snaps_count'), "Should have writer_pending_snaps_count field"
        
        # Basic sanity checks
        assert stats.lmdb_record_count >= 0, f"LMDB record count should be non-negative: {stats.lmdb_record_count}"
        assert stats.sqlite_record_count >= 0, f"SQLite record count should be non-negative: {stats.sqlite_record_count}"
        assert stats.total_hybrid_size_bytes >= 0, f"Total size should be non-negative: {stats.total_hybrid_size_bytes}"
        assert stats.pending_snaps_count >= 0, f"Pending snaps count should be non-negative: {stats.pending_snaps_count}"
        
        print(f"Enhanced stats test - LMDB: {stats.lmdb_record_count}, SQLite: {stats.sqlite_record_count}")
        print(f"Copy lag: {stats.copy_lag}, Pressure: {stats.current_pressure}")
        print(f"Total size: {stats.total_hybrid_size_bytes} bytes")
        
        # Test that we have meaningful data
        assert stats.lmdb_record_count > 0, "Should have some LMDB records"
        
        print("Enhanced statistics test completed successfully!")

    finally:
        await log.stop()


