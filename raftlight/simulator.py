"""Seeded discrete-event transport for the production Raft state machine.

Time is seconds; timers are polled at 5 ms resolution. Messages in flight survive
sender failure and receiver restarts, as packets can on a real network.
"""
from __future__ import annotations

from collections import Counter, deque
from dataclasses import asdict, dataclass, replace
import json
import random
from typing import Callable

from .messages import Message, LogEntry, message_to_dict
from .node import Node, NotLeader, Role
from .state_machine import Command
from .storage import MemoryStorage


@dataclass
class Envelope:
    at: float
    sequence: int
    src: int
    dst: int
    message: Message
    src_generation: int = 0
    dst_generation: int = 0


class SafetyMonitor:
    """Historical oracles survive all simulated node crashes.

    A leader snapshot is compared on election AND when a previously unseen
    commitment is discovered, covering delayed observations of older terms.
    """

    def __init__(self) -> None:
        self.leaders_by_term: dict[int, int] = {}
        self.leadership_history: list[dict] = []
        self.election_history: list[dict] = []
        self.committed: dict[int, LogEntry] = {}
        self.commit_terms: dict[int, int] = {}
        self.commit_times: dict[int, float] = {}
        self.applied: dict[int, LogEntry] = {}
        self.terms: dict[int, int] = {}
        self.votes: dict[tuple[int, int], int] = {}
        self.highest_committed: dict[int, int] = {}
        self.positions: dict[tuple[int, int], tuple[int, int]] = {}
        self.checks: Counter = Counter()

    def observe(self, event: str, node: Node, info: dict, now: float) -> None:
        if event == "election_started":
            self.election_history.append(dict(node=node.node_id, term=node.current_term, time=now))
        elif event == "leader":
            previous = self.leaders_by_term.setdefault(node.current_term, node.node_id)
            assert previous == node.node_id, "Election Safety: two leaders in one term"
            self.leadership_history.append(dict(term=node.current_term, node=node.node_id,
                                                time=now, log=tuple(node.log)))
            self._completeness()
        elif event == "commit":
            index, entry = info["index"], info["entry"]
            assert self.committed.setdefault(index, entry) == entry, "Conflicting commitment"
            self.highest_committed[node.node_id] = max(
                self.highest_committed.get(node.node_id, 0), index)
            self.commit_times.setdefault(index, now)
            # Only leader advancement establishes the commitment term. A
            # follower may first learn an older commitment in a later term.
            if node.role == Role.LEADER:
                self.commit_terms[index] = min(self.commit_terms.get(index, node.current_term),
                                               node.current_term)
                self._completeness()
        elif event == "apply":
            index, entry = info["index"], info["entry"]
            assert self.applied.setdefault(index, entry) == entry, "State Machine Safety"
            assert index <= node.commit_index and node.log[index] == entry

    def _completeness(self) -> None:
        for election in self.leadership_history:
            for index, term in self.commit_terms.items():
                if election["term"] > term:
                    assert len(election["log"]) > index, "Leader Completeness: missing entry"
                    assert election["log"][index] == self.committed[index], "Leader Completeness"

    def check(self, cluster: Cluster) -> None:
        nodes = list(cluster.nodes.values())
        for node in nodes:
            state = cluster.stores[node.node_id].load()
            assert state.current_term >= self.terms.get(node.node_id, 0), "Persistent term regression"
            self.terms[node.node_id] = state.current_term
            key = (node.node_id, state.current_term)
            if key in self.votes:
                # Clearing a durable vote is already a violation, even before
                # a different candidate exploits it to obtain a second vote.
                assert state.voted_for == self.votes[key], "Persisted vote changed or cleared"
            elif state.voted_for is not None:
                assert self.votes.setdefault(key, state.voted_for) == state.voted_for, "Double vote"
            assert node.current_term == state.current_term
            assert node.voted_for == state.voted_for and tuple(node.log) == tuple(state.log)
            assert 0 <= node.last_applied <= node.commit_index < len(node.log)
            incarnation = (node.node_id, cluster.generations[node.node_id])
            previous_commit, previous_applied = self.positions.get(incarnation, (0, 0))
            assert node.commit_index >= previous_commit, "Commit index regressed within an incarnation"
            assert node.last_applied >= previous_applied, "Application index regressed within an incarnation"
            self.positions[incarnation] = (node.commit_index, node.last_applied)
            # Volatile positions legitimately reset on restart. The previously
            # committed durable prefix does not: checking it independently of
            # last_applied catches corruption before recovery reapplies entries.
            highest = self.highest_committed.get(node.node_id, 0)
            assert len(state.log) > highest, "Previously committed durable prefix was truncated"
            for index in range(1, highest + 1):
                assert state.log[index] == self.committed[index], "Previously committed durable prefix changed"
            expected: dict[str, str] = {}
            for index in range(1, node.last_applied + 1):
                entry = node.log[index]
                assert self.applied[index] == entry, "Applied history changed"
                command = entry.command
                if command.op == "set":
                    expected[command.key] = command.value
                elif command.op == "delete":
                    expected.pop(command.key, None)
            assert node.machine == expected, "State machine differs from applied prefix"
            if node.role == Role.LEADER and node.node_id in cluster.alive:
                for peer in node.peers:
                    assert 0 <= node.match_index[peer] < len(node.log)
                    assert node.match_index[peer] + 1 <= node.next_index[peer] <= len(node.log)
            if node.node_id not in cluster.alive:
                assert cluster.crash_states[node.node_id] == cluster._node_snapshot(node), "Crashed node mutated"
        # Highest shared (index,term) implies all lower shared matches too.
        for offset, left in enumerate(nodes):
            for right in nodes[offset + 1:]:
                for index in range(min(len(left.log), len(right.log)) - 1, -1, -1):
                    if left.log[index].term == right.log[index].term:
                        assert left.log[:index + 1] == right.log[:index + 1], "Log Matching"
                        break
        for name in ("election_safety", "log_matching", "leader_completeness",
                     "state_machine_safety", "persistent_state", "index_bounds", "crash_isolation",
                     "committed_prefix_retention", "position_monotonicity"):
            self.checks[name] += 1


class Cluster:
    """One consensus engine, a controllable transport, and independent oracles."""

    def __init__(self, size: int = 3, seed: int = 1, loss: float = 0.0,
                 delay: tuple[float, float] = (0.001, 0.010), duplicate_rate: float = 0.0):
        if size not in (3, 5):
            raise ValueError("RaftLight supports fixed clusters of 3 or 5 nodes")
        if not 0 <= loss <= 1 or not 0 <= duplicate_rate <= 1 or not 0 <= delay[0] <= delay[1]:
            raise ValueError("Invalid network parameters")
        self.size, self.seed, self.now = size, seed, 0.0
        self.rng = random.Random(seed)
        self.loss, self.delay, self.duplicate_rate = loss, delay, duplicate_rate
        self.nodes: dict[int, Node] = {}
        self.stores = {i: MemoryStorage() for i in range(1, size + 1)}
        self.alive = set(self.stores)
        self.generations = {i: 0 for i in self.stores}
        self.links = {(a, b): True for a in self.stores for b in self.stores if a != b}
        self.queue: list[Envelope] = []
        self.sequence = 0
        self.stats: Counter = Counter()
        self.events: list[dict] = []
        self.rpc_history: deque[dict] = deque(maxlen=200)
        self.monitor = SafetyMonitor()
        self.crash_states: dict[int, dict] = {}
        self._next_tick = .005
        for i in self.stores:
            self._new_node(i)
        self.assert_safety()

    def _new_node(self, node_id: int) -> None:
        self.nodes[node_id] = Node(node_id, [i for i in self.stores if i != node_id],
                                   self.stores[node_id], random.Random(self.rng.getrandbits(64)),
                                   lambda dst, msg: self._send(node_id, dst, msg),
                                   self._observe, now=self.now)

    def _observe(self, event: str, node: Node, info: dict) -> None:
        if event == "election_started":
            self.stats["election_attempts"] += 1
        elif event == "leader":
            self.stats["elections"] += 1
        self.monitor.observe(event, node, info, self.now)

    def _record(self, action: str, **info) -> None:
        self.events.append(dict(time=self.now, action=action, **info))

    def _send(self, src: int, dst: int, message: Message) -> None:
        self.stats["rpc_sent"] += 1
        self.stats[type(message).__name__ + "_sent"] += 1
        if not self.links[src, dst] or self.rng.random() < self.loss:
            self.stats["rpc_dropped"] += 1
            return
        for copy in range(1 + int(self.rng.random() < self.duplicate_rate)):
            self.sequence += 1
            self.queue.append(Envelope(self.now + self.rng.uniform(*self.delay), self.sequence,
                                       src, dst, message, self.generations[src], self.generations[dst]))
        self.queue.sort(key=lambda e: (e.at, e.sequence))

    def _deliver(self, envelope: Envelope) -> None:
        self.rpc_history.append(dict(time=self.now, src=envelope.src, dst=envelope.dst,
                                     message=message_to_dict(envelope.message)))
        if envelope.dst in self.alive and self.links[envelope.src, envelope.dst]:
            self.nodes[envelope.dst].receive(envelope.src, envelope.message, self.now)
            self.stats["rpc_delivered"] += 1
        else:
            self.stats["rpc_dropped"] += 1
        self.assert_safety()

    def step(self) -> bool:
        if self.queue and self.queue[0].at <= self._next_tick:
            env = self.queue.pop(0)
            self.now = max(self.now, env.at)
            self._deliver(env)
        else:
            self.now = max(self.now, self._next_tick)
            self._next_tick = self.now + .005
            for i in sorted(self.alive):
                self.nodes[i].tick(self.now)
                self.assert_safety()
        return True

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("Time cannot run backwards")
        end = self.now + seconds
        while min(self.queue[0].at if self.queue else float("inf"), self._next_tick) <= end:
            self.step()
        self.now = end

    def wait_for(self, predicate: Callable[[], bool], timeout: float = 3.0) -> bool:
        end = self.now + timeout
        while not predicate() and self.now < end:
            next_event = min(self.queue[0].at if self.queue else float("inf"), self._next_tick)
            if next_event > end:
                self.now = end
                break
            self.step()
        return bool(predicate())

    def deliver_message(self, index: int = 0) -> bool:
        if not self.queue:
            return False
        # Explicit adversarial scheduling can deliver a packet early, including
        # out of FIFO order. Ordinary advance() honors its sampled delay.
        env = self.queue.pop(index % len(self.queue))
        self._record("deliver", sequence=env.sequence)
        self._deliver(env)
        return True

    def drop_message(self, index: int = 0) -> bool:
        if not self.queue:
            return False
        env = self.queue.pop(index % len(self.queue))
        self.stats["rpc_dropped"] += 1
        self._record("drop", sequence=env.sequence)
        self.assert_safety()
        return True

    def duplicate_message(self, index: int = 0) -> bool:
        if not self.queue:
            return False
        env = self.queue[index % len(self.queue)]
        self.sequence += 1
        self.queue.append(replace(env, sequence=self.sequence, at=max(self.now, env.at) + .001))
        self.queue.sort(key=lambda e: (e.at, e.sequence))
        self._record("duplicate", sequence=env.sequence)
        return True

    def delay_message(self, index: int = 0, seconds: float = .3) -> bool:
        if not self.queue:
            return False
        if seconds <= 0:
            raise ValueError("Delay must be positive")
        env = self.queue[index % len(self.queue)]
        env.at += seconds
        self.queue.sort(key=lambda e: (e.at, e.sequence))
        self._record("delay", sequence=env.sequence, seconds=seconds)
        return True

    def partition(self, groups: list[list[int]]) -> bool:
        flat = [i for group in groups for i in group]
        if sorted(flat) != sorted(self.stores):
            raise ValueError("Partition groups must cover every node exactly once")
        membership = {i: g for g, group in enumerate(groups) for i in group}
        links = {(a, b): membership[a] == membership[b] for a, b in self.links}
        changed = links != self.links
        self.links = links
        self._record("partition", groups=groups)
        self.assert_safety()
        return changed

    def set_link(self, src: int, dst: int, enabled: bool) -> bool:
        changed = self.links[src, dst] != enabled
        self.links[src, dst] = enabled
        self._record("link", src=src, dst=dst, enabled=enabled)
        return changed

    def heal(self) -> bool:
        changed = not all(self.links.values())
        self.links = dict.fromkeys(self.links, True)
        self._record("heal")
        return changed

    def crash(self, node_id: int) -> bool:
        if node_id not in self.alive:
            return False
        self.alive.remove(node_id)
        self.crash_states[node_id] = self._node_snapshot(self.nodes[node_id])
        self._record("crash", node=node_id)
        self.assert_safety()
        return True

    def restart(self, node_id: int) -> bool:
        if node_id in self.alive:
            return False
        self.generations[node_id] += 1
        self._new_node(node_id)
        self.alive.add(node_id)
        del self.crash_states[node_id]
        self._record("restart", node=node_id)
        self.assert_safety()
        return True

    @property
    def leaders(self) -> list[Node]:
        return [self.nodes[i] for i in sorted(self.alive) if self.nodes[i].role == Role.LEADER]

    def leader(self) -> Node | None:
        return max(self.leaders, key=lambda n: (n.current_term, n.node_id), default=None)

    def submit(self, command: Command, leader_id: int | None = None) -> tuple[int, int] | None:
        node = self.nodes.get(leader_id) if leader_id is not None else self.leader()
        self._record("submit", command=asdict(command), node=node.node_id if node else None)
        self.stats["client_submitted"] += 1
        if node is None or node.node_id not in self.alive:
            self.stats["client_rejected"] += 1
            return None
        try:
            index = node.propose(command, self.now)
        except NotLeader:
            self.stats["client_rejected"] += 1
            return None
        self.assert_safety()
        return node.node_id, index

    def is_committed(self, ticket: tuple[int, int] | None, command: Command) -> bool:
        if ticket is None:
            return False
        entry = self.monitor.committed.get(ticket[1])
        return entry is not None and entry.command == command

    def converged(self) -> bool:
        leader = self.leader()
        return leader is not None and len(self.alive) == self.size and all(
            n.log == leader.log and n.commit_index == len(leader.log) - 1 and
            n.machine == leader.machine for n in self.nodes.values())

    def stabilize(self, timeout: float = 5.0) -> bool:
        self.heal()
        self.loss, self.delay, self.duplicate_rate = 0.0, (.001, .010), 0.0
        for i in sorted(self.stores):
            self.restart(i)
        return self.wait_for(self.converged, timeout)

    def assert_safety(self) -> None:
        try:
            self.monitor.check(self)
        except AssertionError as error:
            # No suppression: a caller can persist this JSON as a counterexample.
            error.add_note(json.dumps(self.snapshot(), default=str))
            raise

    @staticmethod
    def _node_snapshot(node: Node) -> dict:
        return dict(role=node.role.value, term=node.current_term, vote=node.voted_for,
                    commit=node.commit_index, applied=node.last_applied,
                    log=[asdict(e) for e in node.log], machine=dict(node.machine))

    def snapshot(self) -> dict:
        return dict(seed=self.seed, size=self.size, time=self.now, alive=sorted(self.alive),
                    nodes={i: self._node_snapshot(n) for i, n in self.nodes.items()},
                    leaders=[n.node_id for n in self.leaders],
                    links={f"{a}->{b}": enabled for (a, b), enabled in self.links.items()},
                    events=self.events, rpc_history=list(self.rpc_history),
                    queue=[dict(at=e.at, sequence=e.sequence, src=e.src, dst=e.dst,
                                message=message_to_dict(e.message)) for e in self.queue])
