"""
pytest configuration for the raftengine tests.

This file is automatically loaded by pytest and provides
shared fixtures and configuration for all tests.
"""
import os
import pytest
import logging

# Set ipdb as the default breakpoint() debugger
# This makes breakpoint() use ipdb.set_trace instead of pdb.set_trace
os.environ['PYTHONBREAKPOINT'] = 'ipdb.set_trace'

def pytest_configure(config):
    """
    Configure pytest to use ipdb for --pdb flag.
    
    This hook is called after command line options have been parsed
    and before test collection starts.
    """
    # If --pdb is specified, configure pytest to use ipdb
    if config.option.usepdb:
        try:
            import ipdb
            # Replace the default pdb with ipdb in pytest's debugging module
            import _pytest.debugging
            import pdb
            
            # Override pdb.post_mortem to use ipdb.post_mortem
            pdb.post_mortem = ipdb.post_mortem
            pdb.set_trace = ipdb.set_trace
            
            # Also override in the debugging module directly
            _pytest.debugging.post_mortem = ipdb.post_mortem
            
        except ImportError:
            # Fall back to pdb if ipdb is not available
            pass

@pytest.fixture(scope="session", autouse=True)
def configure_breakpoint():
    """
    Automatically configure ipdb as the breakpoint debugger for all tests.
    
    This fixture runs automatically for all tests and ensures that
    breakpoint() uses ipdb.set_trace, providing the same debugging
    experience as the run_tests.sh script.
    
    Usage in tests:
        def test_something():
            breakpoint()  # This will use ipdb instead of pdb
            assert True
    
    Also ensures that pytest --pdb uses ipdb for breakpoint on exception.
    """
    # Ensure the environment variable is set
    os.environ['PYTHONBREAKPOINT'] = 'ipdb.set_trace'
    
    try:
        from raftengine.deck.log_control import LogController

        extras = [('test_code', ''),]

        try:
            controller = LogController.get_controller()
        except Exception:
            # If no controller exists, create one
            controller = LogController.make_controller(additional_loggers=extras)
        if os.environ.get('RAFTLOG_DEBUG_LOGGING') == '1':
            controller.set_default_level('debug')
            #from pprint import pprint
            #pprint(controller.to_dict_config())
    except ImportError:
        # If LogController is not available, skip debug logging setup
        pass
    
    yield
    # Cleanup after tests (optional)
    pass
