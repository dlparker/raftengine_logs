#!/usr/bin/env python
import asyncio
import logging
import shutil
import traceback
from pathlib import Path
import time
import json
import multiprocessing
import os
from random import randint

import pytest
from raftengine.api.log_api import LogRec
from raftengine.api.log_api import LogRec
from hybrid_log.hybrid_log import HybridLog
from common import (inner_log_test_basic, inner_log_perf_run,
                    inner_log_test_deletes, inner_log_test_snapshots,
                    inner_log_test_configs
                    )


class HL(HybridLog):
    
    async def start(self):
        await self.lmdb_log.start()
        await self.sqlite_log.start()
        await self.sqlwriter.start(self.sqlwriter_callback, self.handle_writer_error, inprocess=True)

async def test_mp_clients():

    path = Path('/tmp', f"test_log_1")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    log = HybridLog(path, hold_count=1000, push_snap_size=1000)
    await log.start()

    try:
        await log.set_term(1)
        # modify the threasholds
        stats = await log.get_stats()
        # write one record first to bypass annoying first_index = None problem
        for index in range(1, 10000+1):
            new_rec = LogRec(command=f"add {index}", serial=index)
            rec = await log.append(new_rec)
        
        # make sure commit and apply get updated properly in sqlite
        await asyncio.sleep(0.01)
        snap = await log.lmdb_log.get_snapshot()
        assert snap is not None
        assert snap.index + 1 == await log.lmdb_log.get_first_index()
        assert await log.sqlite_log.get_last_index() >= snap.index

    finally:
        await log.stop()


class LogServer:
    """Async socket server that wraps HybridLog to accept command requests"""
    
    def __init__(self, log_path, port):
        self.log_path = log_path
        self.port = port

        self.server = None
        self.running = False
        
    async def start(self):
        """Initialize the log and start the server"""
        self.log = HL(self.log_path)
        await self.log.start()
        await self.log.set_term(1)
        
        self.server = await asyncio.start_server(
            self.handle_client, 'localhost', self.port
        )
        self.running = True
        
    async def stop(self):
        """Stop the server and cleanup"""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self.log:
            await self.log.stop()
            
    async def handle_client(self, reader, writer):
        """Handle individual client connections"""
        client_addr = writer.get_extra_info('peername')
        logger = logging.getLogger('test_server')
        logger.debug(f"Client connected: {client_addr}")
        
        try:
            while self.running:
                # Read message length (20 bytes)
                len_data = await reader.read(20)
                if not len_data:
                    break
                    
                msg_len = int(len_data.decode().strip())
                if msg_len == 0:
                    break
                    
                # Read the actual message
                data = await reader.read(msg_len)
                if not data:
                    break
                    
                # Parse command
                try:
                    command = json.loads(data.decode())
                    await self.process_command(command, writer)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON from {client_addr}")
                    break
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error handling client {client_addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.debug(f"Client disconnected: {client_addr}")
            
    async def process_command(self, command, writer):
        """Process a command and send response"""
        try:
            if command.get('command') == 'add':
                # Create LogRec and append to log
                value = command.get('value', 0)
                client_id = command.get('client_id', 'unknown')
                log_rec = LogRec(command=f"add {value}", serial=value)
                result = await self.log.append(log_rec)
                
                # Send success response
                response = {'status': 'success', 'index': result.index}
            else:
                response = {'status': 'error', 'message': 'Unknown command'}
                
        except Exception as e:
            response = {'status': 'error', 'message': str(e)}
            
        # Send response back to client
        response_data = json.dumps(response).encode()
        response_len = f"{len(response_data):20d}".encode()
        writer.write(response_len)
        writer.write(response_data)
        await writer.drain()


def log_server_process(log_path, port, ready_event):
    """Process function to run the log server"""
    async def run_server():
        server = LogServer(log_path, port)
        try:
            await server.start()
            ready_event.set()  # Signal that server is ready
            
            # Keep the server running until interrupted
            while server.running:
                await asyncio.sleep(0.1)
        except:
            print('\n\n ------------- Server error ------------------- \n\n')
            print(traceback.format_exc())
            print('\n\n ---------------------------------------------- \n\n')
            
        finally:
            await server.stop()
            
    try:
        asyncio.run(run_server())
    except:
        print('\n\n ------------- Server error ------------------- \n\n')
        print(traceback.format_exc())
        print('\n\n ---------------------------------------------- \n\n')
    print("server exiting")

async def log_client(port, client_id, num_commands=100):
    """Client function that connects to server and sends commands"""
    try:
        reader, writer = await asyncio.open_connection('localhost', port)
        
        for i in range(num_commands):
            # Create command
            command = {
                'command': 'add',
                'value': i + 1,
                'client_id': client_id
            }
            
            # Send command
            command_data = json.dumps(command).encode()
            command_len = f"{len(command_data):20d}".encode()
            writer.write(command_len)
            writer.write(command_data)
            await writer.drain()
            
            # Read response
            len_data = await reader.read(20)
            if not len_data:
                break
            msg_len = int(len_data.decode().strip())
            response_data = await reader.read(msg_len)
            response = json.loads(response_data.decode())
            
            if response['status'] != 'success':
                raise Exception(f"Command failed: {response}")
    except ConnectionResetError:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass


def client_process(port, client_id, num_commands):
    try:
        asyncio.run(log_client(port, client_id, num_commands))
    except ConnectionResetError:
        pass


async def test_hybrid_socket_mp_clients():
    """Test HybridLog with X client processes connecting via async sockets"""
    
    # Setup test directory
    path = Path('/tmp', f"test_log_socket")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
    
    # Choose a random port
    port = randint(8000, 65000)
    
    # Create multiprocessing coordination
    ready_event = multiprocessing.Event()
    
    # Start the server process
    server_proc = multiprocessing.Process(
        target=log_server_process,
        args=(path, port, ready_event)
    )
    server_proc.start()

    errors = []
    try:
        # Wait for server to be ready
        if not ready_event.wait(timeout=10):
            raise Exception("Server did not start within timeout")
        
        # Small delay to ensure server is fully ready
        await asyncio.sleep(0.1)
        
        # Launch 5 client processes, each sending 100 commands
        client_processes = []
        num_clients = 5
        commands_per_client = 500
        client_timeout = (2 * (commands_per_client / 1000)) * num_clients
        print(f"\nRunning {num_clients} clients for {commands_per_client} ops per\n")
        
        for client_id in range(num_clients):
            proc = multiprocessing.Process(
                target=client_process,
                args=(port, f"client_{client_id}", commands_per_client)
            )
            client_processes.append(proc)
            proc.start()
        
        # Wait for all client processes to complete
        for proc in client_processes:
            proc.join(timeout=client_timeout) 
            if proc.exitcode != 0:
                errors.append(proc.exitcode)
        
        # Allow time for final processing
        await asyncio.sleep(0.5)
        
    finally:
        # Terminate server process
        server_proc.terminate()
        server_proc.join(timeout=5)
        if server_proc.is_alive():
            server_proc.kill()
    
    # Now verify the log contents (similar to existing test)
    log = HybridLog(path)
    await log.start()
    
    try:
        expected_records = num_clients * commands_per_client
        last_index = await log.get_last_index()
        first_index = await log.get_first_index()
        assert last_index == expected_records, f"Expected {expected_records} records, got {last_index}"
        print(f"Expected {expected_records} records, got {last_index}")
        
        # Verify boundary conditions between LMDB and SQLite 
        # (similar to existing test)
        await asyncio.sleep(0.01)
        snap = await log.lmdb_log.get_snapshot()
        
        if snap is not None:
            # Verify proper boundary management between LMDB and SQLite
            lmdb_first = await log.lmdb_log.get_first_index()
            sqlite_last = await log.sqlite_log.get_last_index()
            
            assert snap.index + 1 == lmdb_first, f"LMDB boundary condition failed: snap.index={snap.index}, lmdb_first={lmdb_first}"
            assert sqlite_last >= snap.index, f"SQLite boundary condition failed: sqlite_last={sqlite_last}, snap.index={snap.index}"
        
        # Verify we can read records from both LMDB and SQLite portions
        if first_index is not None:
            first_rec = await log.read(first_index)
            assert first_rec is not None, f"Should be able to read first record at index {first_index}"
            assert first_rec.command.startswith("add "), f"Unexpected command format: {first_rec.command}"
        
        if last_index > 0:
            last_rec = await log.read(last_index)
            assert last_rec is not None, f"Should be able to read last record at index {last_index}"
            assert last_rec.command.startswith("add "), f"Unexpected command format: {last_rec.command}"

        if len(errors) > 0:
            raise Exception(f'{len(errors)} clients failed')
    finally:
        await log.stop()
    
    
