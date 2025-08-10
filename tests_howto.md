# Testing Setup Guide

This guide explains how to set up your environment to run tests with the new package structure.

## Prerequisites

- Python 3.11 or higher
- A clean virtual environment

## Method 1: Install Individual Packages (Recommended for Development)

This method installs each log package in editable mode, allowing you to make changes and see them immediately in tests.

### Step 1: Install Test Dependencies

```bash
pip install pytest>=8.4.1 pytest-asyncio>=1.1.0 pytest-cov>=6.2.1 ipdb>=0.13.13
```

### Step 2: Install raftengine

```bash
pip install "raftengine @ git+https://github.com/dlparker/raftengine.git@v0.2.0"
```

### Step 3: Install Log Packages in Editable Mode

From the project root directory:

```bash
# Install each package in editable mode
pip install -e packages/sqlitelog/
pip install -e packages/memorylog/
pip install -e packages/lmdblog/
pip install -e packages/hybridlog/
```

**Note**: If you encounter license format warnings, they are harmless and the installation will still succeed.

### Step 4: Verify Installation

Test that imports work correctly:

```bash
python -c "from raftengine_logs.sqlite_log import SqliteLog; print('SqliteLog: OK')"
python -c "from raftengine_logs.memory_log import MemoryLog; print('MemoryLog: OK')"
python -c "from raftengine_logs.lmdb_log import LmdbLog; print('LmdbLog: OK')"
python -c "from raftengine_logs.hybrid_log import HybridLog; print('HybridLog: OK')"
```

### Step 5: Run Tests

```bash
# Run all tests
pytest tests/

# Run specific test files
pytest tests/test_sqlite.py
pytest tests/test_memlog.py
pytest tests/test_lmdb.py
pytest tests/test_hybrid.py

# Run basic log implementations (recommended first test)
pytest tests/test_sqlite.py tests/test_lmdb.py tests/test_memlog.py -v

# Run with coverage
pytest tests/ --cov=raftengine_logs --cov-report=html
```

**Note**: The hybrid log tests may fail due to multiprocessing/networking setup issues, but the basic log implementations (SQLite, LMDB, Memory) should all pass.

## Method 2: Install from Root Package (Alternative)

This method uses the root package's development dependencies.

### Step 1: Install Root Package with Dev Dependencies

```bash
# Install the root package with all development dependencies
pip install -e .[dev]
```

Note: This method requires PDM or a compatible tool that understands dependency groups.

## Method 3: Manual Requirements File

If you prefer a requirements file approach, create a `test_requirements.txt`:

```bash
# Create a new requirements file
cat > test_requirements.txt << 'EOF'
# Test framework
pytest>=8.4.1
pytest-asyncio>=1.1.0
pytest-cov>=6.2.1
ipdb>=0.13.13

# Core dependency
raftengine @ git+https://github.com/dlparker/raftengine.git@v0.2.0

# Log packages (editable installs)
-e packages/sqlitelog/
-e packages/memorylog/
-e packages/lmdblog/
-e packages/hybridlog/
EOF

# Install from requirements file
pip install -r test_requirements.txt
```

## Package Structure Overview

After installation, you can import packages as follows:

```python
# SQLite log implementation
from raftengine_logs.sqlite_log import SqliteLog

# Memory log implementation (testing only)
from raftengine_logs.memory_log import MemoryLog

# LMDB log implementation
from raftengine_logs.lmdb_log import LmdbLog

# Hybrid LMDB+SQLite implementation
from raftengine_logs.hybrid_log import HybridLog
from raftengine_logs.hybrid_log import HybridStats
from raftengine_logs.hybrid_log import SqliteWriterControl
```

## Individual Package Installation

You can also install packages individually for production use:

```bash
# Install only what you need
pip install raftengine-logs-sqlite    # SQLite implementation only
pip install raftengine-logs-lmdb      # LMDB implementation only
pip install raftengine-logs-memory    # Memory implementation only
pip install raftengine-logs-hybrid    # Hybrid implementation only
```

## Troubleshooting

### Import Errors

If you get import errors like `ModuleNotFoundError: No module named 'raftengine_logs'`:

1. Verify packages are installed: `pip list | grep raftengine`
2. Check that you're in the correct virtual environment
3. Try reinstalling in editable mode: `pip install -e packages/sqlitelog/`

### LMDB Installation Issues

If LMDB fails to install:

```bash
# On Ubuntu/Debian
sudo apt-get install liblmdb-dev

# On macOS
brew install lmdb

# Then retry
pip install lmdb>=1.7.3
```

### Test Discovery Issues

If pytest can't find tests:

```bash
# Run from project root
cd /path/to/raftengine_logs
pytest tests/

# Or specify the path explicitly
pytest /path/to/raftengine_logs/tests/
```

## Development Workflow

For active development:

1. Make changes to code in `packages/*/src/raftengine_logs/*/`
2. Tests will automatically use your changes (thanks to editable installs)
3. Run specific tests: `pytest tests/test_sqlite.py::test_specific_function`
4. Run with verbose output: `pytest -v tests/`
5. Run with coverage: `pytest --cov=raftengine_logs tests/`

## Performance Tests

To run performance tests with custom loop counts:

```bash
# Set custom loop count for performance tests
RAFTLOG_PERF_LOOPS=1000 pytest tests/test_perf.py
```

## Clean Installation

To start fresh:

```bash
# Remove all raftengine-logs packages
pip uninstall raftengine-logs-sqlite raftengine-logs-memory raftengine-logs-lmdb raftengine-logs-hybrid raftengine-logs -y

# Reinstall in editable mode
pip install -e packages/sqlitelog/ -e packages/memorylog/ -e packages/lmdblog/ -e packages/hybridlog/
```