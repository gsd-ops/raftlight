"""Transport-independent, single-threaded Raft state machine (Figure 2).

The caller serializes tick/receive/propose and supplies monotonically increasing
seconds. No wall clock, sleeps, sockets, or background tasks live in this module.
Log index zero is a permanent term-zero no-op; real entries start at index one.
"""

from collections import OrderedDict
from enum import Enum
import math
import random
from typing import Any, Callable

from .messages import (AppendEntries, AppendResponse, LogEntry, Message,
                       RequestVote, VoteResponse, _integer)
from .state_machine import Command, apply_command
from .storage import PersistentState, Storage, StorageFailure


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeader(RuntimeError):
    def __init__(self, leader_id: int | None = None) -> None:
        self.leader_id = leader_id
        super().__init__(f"not leader; known leader: {leader_id}")


Observer = Callable[[str, "Node", dict[str, Any]], None]


class Node:
    ELECTION_TIMEOUT = (0.150, 0.300)
    HEARTBEAT_INTERVAL = 0.050
    MAX_PENDING_PER_PEER = 256
    MAX_APPEND_ENTRIES = 8

    def __init__(self, node_id: int, peers: list[int], storage: Storage,
                 rng: random.Random, send: Callable[[int, Message], None],
                 observe: Observer | None = None, now: float = 0,
                 *,
                 election_timeout: tuple[float, float] = ELECTION_TIMEOUT,
                 heartbeat_interval: float = HEARTBEAT_INTERVAL) -> None:
        _integer(node_id, "node_id")
        for peer in peers:
            _integer(peer, "peer")
        if node_id in peers or len(set(peers)) != len(peers):
            raise ValueError("peers must be unique and exclude this node")
        low, high = election_timeout
        if (isinstance(low, bool) or isinstance(high, bool) or isinstance(heartbeat_interval, bool)
                or not isinstance(low, (int, float)) or not isinstance(high, (int, float))
                or not isinstance(heartbeat_interval, (int, float))
                or not (0 < low <= high and math.isfinite(low) and math.isfinite(high))
                or not (0 < heartbeat_interval and math.isfinite(heartbeat_interval))):
            raise ValueError("election_timeout and heartbeat_interval must be positive finite seconds")
        self.node_id = node_id
        self.peers = tuple(peers)
        self.storage = storage
        self.rng = rng
        self.send = send
        self.observe = observe
        self.election_timeout = (float(low), float(high))
        self.heartbeat_interval = float(heartbeat_interval)
        state = storage.load()
        self.current_term = state.current_term
        self.voted_for = state.voted_for
        self.log = list(state.log)
        self.role = Role.FOLLOWER
        self.leader_id: int | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.machine: dict[str, str] = {}
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self._votes: set[int] = set()
        self._pending: dict[int, OrderedDict[int, tuple[int, int]]] = {}
        self._request_id = 0
        self._storage_failure: Exception | None = None
        self._now = float(now)
        self._set_now(now)
        self.election_deadline = 0.0
        self.heartbeat_deadline = 0.0
        self._reset_election_timer()

    @property
    def quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    @property
    def last_log_index(self) -> int:
        return len(self.log) - 1

    def _set_now(self, now: float) -> None:
        if self._storage_failure is not None:
            raise StorageFailure("node stopped after a persistence failure") from self._storage_failure
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise ValueError("time must be finite seconds")
        if now < self._now:
            raise ValueError("node time cannot move backwards")
        self._now = float(now)

    def _emit(self, event: str, **information: Any) -> None:
        if self.observe is not None:
            self.observe(event, self, information)

    def _persist(self, log: list[LogEntry] | None = None) -> None:
        try:
            self.storage.save(PersistentState(self.current_term, self.voted_for,
                                               tuple(self.log if log is None else log)))
        except (OSError, ValueError) as error:
            # This is a fatal durability failure, never an ordinary lost RPC.
            # Latch it so an adapter cannot accidentally resume this instance.
            self._storage_failure = error
            raise StorageFailure("could not durably persist Raft state") from error

    def _reset_election_timer(self) -> None:
        self.election_deadline = self._now + self.rng.uniform(*self.election_timeout)

    def _become_follower(self, term: int, leader_id: int | None = None) -> None:
        if term < self.current_term:
            raise ValueError("cannot become follower in an older term")
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist()
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self._votes.clear()
        self.next_index.clear()
        self.match_index.clear()
        self._pending.clear()
        self._reset_election_timer()

    def tick(self, now: float) -> None:
        self._set_now(now)
        if self.role == Role.LEADER:
            if now >= self.heartbeat_deadline:
                self._broadcast_append()
        elif now >= self.election_deadline:
            self.start_election(now)

    def start_election(self, now: float) -> None:
        self._set_now(now)
        self.current_term += 1
        self.voted_for = self.node_id
        self.role = Role.CANDIDATE
        self.leader_id = None
        self._votes = {self.node_id}
        self.next_index.clear()
        self.match_index.clear()
        self._pending.clear()
        self._persist()  # Durable self-vote precedes every RequestVote.
        self._reset_election_timer()
        self._emit("election_started", term=self.current_term)
        if len(self._votes) >= self.quorum:
            self._become_leader()
            return
        request = RequestVote(self.current_term, self.node_id,
                              self.last_log_index, self.log[-1].term)
        for peer in self.peers:
            self.send(peer, request)

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self.next_index = {peer: len(self.log) for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self._pending = {peer: OrderedDict() for peer in self.peers}
        # A current-term no-op makes prior committed data recoverable without a
        # new client write and permits safe indirect commitment of older entries.
        self.log.append(LogEntry(self.current_term, Command("noop")))
        self._persist()
        self._emit("leader", term=self.current_term)
        self._advance_commit()
        self._broadcast_append()

    def propose(self, command: Command, now: float) -> int:
        """Append a proposal, returning its index, NOT a commit acknowledgement.

        The client adapter must wait for this exact entry to commit. A timeout
        means unknown outcome; no exactly-once client retry guarantee is provided.
        """
        self._set_now(now)
        if self.role != Role.LEADER:
            raise NotLeader(self.leader_id)
        if not isinstance(command, Command):
            raise ValueError("proposal must be a Command")
        self.log.append(LogEntry(self.current_term, command))
        self._persist()
        index = self.last_log_index
        self._advance_commit()
        self._broadcast_append()
        return index

    def receive(self, src: int, message: Message, now: float) -> None:
        """Handle a validated member RPC; malformed identity has no side effect."""
        if type(src) is not int or src not in self.peers:
            raise ValueError("RPC source is not another configured member")
        if type(message) not in (RequestVote, VoteResponse, AppendEntries, AppendResponse):
            raise ValueError("unsupported RPC type")
        if isinstance(message, RequestVote) and message.candidate_id != src:
            raise ValueError("candidate identity does not match RPC source")
        if isinstance(message, AppendEntries) and message.leader_id != src:
            raise ValueError("leader identity does not match RPC source")
        self._set_now(now)
        if message.term > self.current_term:
            self._become_follower(message.term)
        if isinstance(message, RequestVote):
            self._on_request_vote(src, message)
        elif isinstance(message, VoteResponse):
            if (message.term == self.current_term and self.role == Role.CANDIDATE
                    and message.vote_granted):
                self._votes.add(src)
                if len(self._votes) >= self.quorum:
                    self._become_leader()
        elif isinstance(message, AppendEntries):
            self._on_append_entries(src, message)
        elif isinstance(message, AppendResponse):
            self._on_append_response(src, message)

    def _on_request_vote(self, src: int, request: RequestVote) -> None:
        fresh = ((request.last_log_term, request.last_log_index)
                 >= (self.log[-1].term, self.last_log_index))
        grant = (request.term == self.current_term and fresh
                 and self.voted_for in (None, src))
        if grant:
            self.voted_for = src
            self._persist()  # Never acknowledge an unpersisted vote.
            self._reset_election_timer()
        self.send(src, VoteResponse(self.current_term, grant))

    def _reject_append(self, src: int, request: AppendEntries,
                       conflict_index: int, conflict_term: int | None = None) -> None:
        self.send(src, AppendResponse(self.current_term, False,
                                     conflict_index=conflict_index,
                                     conflict_term=conflict_term,
                                     request_id=request.request_id))

    def _on_append_entries(self, src: int, request: AppendEntries) -> None:
        if request.term < self.current_term:
            self._reject_append(src, request, len(self.log))
            return
        if self.role != Role.FOLLOWER:
            self._become_follower(request.term, src)
        self.leader_id = src
        self._reset_election_timer()
        if request.prev_log_index >= len(self.log):
            self._reject_append(src, request, len(self.log))
            return
        if self.log[request.prev_log_index].term != request.prev_log_term:
            term = self.log[request.prev_log_index].term
            first = request.prev_log_index
            while first > 1 and self.log[first - 1].term == term:
                first -= 1
            self._reject_append(src, request, first, term)
            return
        replacement_at: int | None = None
        for offset, entry in enumerate(request.entries, request.prev_log_index + 1):
            if offset < len(self.log):
                if self.log[offset] == entry:
                    continue
                # Equal-term different commands are impossible for correct Raft
                # senders. Reject corrupt input rather than silently rewriting it.
                if self.log[offset].term == entry.term or offset <= self.commit_index:
                    self._reject_append(src, request, max(1, offset))
                    return
            replacement_at = offset
            break
        if replacement_at is not None:
            # Plan the complete suffix replacement first. A rejected request
            # never leaves partially modified in-memory or durable log state.
            offset_in_request = replacement_at - request.prev_log_index - 1
            replacement = self.log[:replacement_at] + list(request.entries[offset_in_request:])
            self._persist(replacement)
            self.log = replacement
        matched = request.prev_log_index + len(request.entries)
        # Cap at the prefix verified by THIS request, not the follower's possibly
        # divergent trailing suffix (especially important for delayed heartbeats).
        new_commit = min(request.leader_commit, matched)
        if new_commit > self.commit_index:
            self._set_commit(new_commit)
        self.send(src, AppendResponse(self.current_term, True, matched,
                                     request_id=request.request_id))

    def _send_append(self, peer: int) -> None:
        next_index = self.next_index[peer]
        previous = next_index - 1
        entries = tuple(self.log[next_index:next_index + self.MAX_APPEND_ENTRIES])
        end = previous + len(entries)
        self._request_id += 1
        request_id = self._request_id
        pending = self._pending[peer]
        pending[request_id] = (previous, end)
        if len(pending) > self.MAX_PENDING_PER_PEER:
            pending.popitem(last=False)
        self.send(peer, AppendEntries(self.current_term, self.node_id,
                                      previous, self.log[previous].term,
                                      entries, self.commit_index,
                                      request_id))

    def _broadcast_append(self) -> None:
        self.heartbeat_deadline = self._now + self.heartbeat_interval
        for peer in self.peers:
            self._send_append(peer)

    def _on_append_response(self, src: int, response: AppendResponse) -> None:
        if response.term != self.current_term or self.role != Role.LEADER:
            return
        sent = self._pending[src].pop(response.request_id, None)
        if sent is None:
            return  # Duplicate, obsolete, or never-issued response.
        previous, end = sent
        if response.success:
            if response.match_index != end:
                return  # A response cannot acknowledge entries not in its RPC.
            self.match_index[src] = max(self.match_index[src], end)
            self.next_index[src] = max(self.next_index[src], end + 1)
            committed = self._advance_commit()
            if committed:
                self._broadcast_append()
            elif self.next_index[src] <= self.last_log_index:
                self._send_append(src)
        else:
            # A late rejection must not undo a newer success or a newer backoff.
            if previous != self.next_index[src] - 1 or previous < self.match_index[src]:
                return
            target = response.conflict_index
            if response.conflict_term is not None:
                matches = [index for index, entry in enumerate(self.log)
                           if entry.term == response.conflict_term]
                if matches:
                    target = matches[-1] + 1
            self.next_index[src] = max(self.match_index[src] + 1,
                                       min(self.next_index[src] - 1, target))
            self._send_append(src)

    def _advance_commit(self) -> bool:
        """Count replicas only for an entry from this leader's current term."""
        if self.role != Role.LEADER:
            return False
        matched = sorted([self.last_log_index, *self.match_index.values()], reverse=True)
        candidate = matched[self.quorum - 1]
        if candidate > self.commit_index and self.log[candidate].term == self.current_term:
            self._set_commit(candidate)
            return True
        return False

    def _set_commit(self, index: int) -> None:
        previous = self.commit_index
        self.commit_index = index
        for position in range(previous + 1, index + 1):
            self._emit("commit", index=position, entry=self.log[position], term=self.current_term)
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            apply_command(self.machine, entry.command)
            self._emit("apply", index=self.last_applied, entry=entry, term=self.current_term)
