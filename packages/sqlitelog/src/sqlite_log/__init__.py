"""SQLite-based log implementation for raftengine."""

from .sqlite_log import SqliteLog

__version__ = "0.5.0"
__all__ = ['SqliteLog']