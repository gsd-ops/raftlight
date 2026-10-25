"""Small direct-node harness for isolating Raft Figure 2 rules."""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from raftlight.node import Node
from raftlight.storage import MemoryStorage, PersistentState


@dataclass
class Harness:
    sent: list = field(default_factory=list)
    observed: list = field(default_factory=list)

    def node(self, node_id=1, peers=(2, 3), state=None, now=0.0):
        storage = MemoryStorage(state)
        node = Node(node_id, peers, storage, random.Random(node_id),
                    lambda *args: self.sent.append(args),
                    lambda *args: self.observed.append(args), now=now)
        return node, storage


def durable(term=0, voted_for=None, entries=()):
    """Build a durable log including its dummy index zero."""
    from raftlight.messages import LogEntry
    from raftlight.state_machine import Command
    return PersistentState(term, voted_for,
                           (LogEntry(0, Command("noop")), *entries))
