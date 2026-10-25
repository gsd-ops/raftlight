"""RaftLight: standard crash-fault Raft for small, fixed-membership clusters."""

from .messages import LogEntry
from .node import Node, NotLeader, Role
from .state_machine import Command
from .storage import FileStorage, MemoryStorage, PersistentState

__all__ = ["Command", "FileStorage", "LogEntry", "MemoryStorage", "Node",
           "NotLeader", "PersistentState", "Role"]
__version__ = "1.0.0"
