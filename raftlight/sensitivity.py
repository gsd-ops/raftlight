"""Five isolated protocol defects used only for oracle-sensitivity experiments.

The production Node, storage classes, and SafetyMonitor are not modified. Each
defect runs against the same deterministic fixture and schedule as a correct
Node control. These are five hand-written sensitivity experiments, not a
mutation-testing framework. Only an assertion raised inside the production
SafetyMonitor counts as detection; scenario errors and storage errors propagate.
"""

from __future__ import annotations

from dataclasses import dataclass
import random
import traceback
from typing import Callable

from .messages import (AppendEntries, AppendResponse, LogEntry, Message,
                       RequestVote, VoteResponse, message_to_dict)
from .node import Node, Role
from .simulator import Cluster
from .state_machine import Command
from .storage import MemoryStorage, PersistentState


class NoElectionRestriction(Node):
    """Defect: grant an otherwise eligible vote without checking log freshness."""

    def _on_request_vote(self, src: int, request: RequestVote) -> None:
        grant = request.term == self.current_term and self.voted_for in (None, src)
        if grant:
            self.voted_for = src
            self._persist()
            self._reset_election_timer()
        self.send(src, VoteResponse(self.current_term, grant))


class DoubleVote(Node):
    """Defect: ignore an existing same-term vote when a fresh candidate asks."""

    def _on_request_vote(self, src: int, request: RequestVote) -> None:
        fresh = ((request.last_log_term, request.last_log_index)
                 >= (self.log[-1].term, self.last_log_index))
        grant = request.term == self.current_term and fresh
        if grant:
            self.voted_for = src
            self._persist()
            self._reset_election_timer()
        self.send(src, VoteResponse(self.current_term, grant))


class ForgetVoteOnRestart(Node):
    """Defect: acknowledge an external vote without persisting voted_for.

    Higher-term transitions still persist term/log through the ordinary core.
    The missing save is detected before the scheduled crash. A separate witness
    actually reconstructs the node and demonstrates the resulting second vote.
    """

    def _on_request_vote(self, src: int, request: RequestVote) -> None:
        fresh = ((request.last_log_term, request.last_log_index)
                 >= (self.log[-1].term, self.last_log_index))
        grant = (request.term == self.current_term and fresh
                 and self.voted_for in (None, src))
        if grant:
            self.voted_for = src
            # Deliberate defect: the durable vote write is omitted.
            self._reset_election_timer()
        self.send(src, VoteResponse(self.current_term, grant))


class NaiveCommit(Node):
    """Defect: count an older-term quorum without requiring a current-term entry."""

    def _advance_commit(self) -> bool:
        if self.role != Role.LEADER:
            return False
        matched = sorted([self.last_log_index, *self.match_index.values()], reverse=True)
        candidate = matched[self.quorum - 1]
        if candidate > self.commit_index:
            self._set_commit(candidate)
            return True
        return False


class CommitBeyondMatch(Node):
    """Defect: after a successful append, commit through the local trailing log.

    The correct handler first commits only its verified prefix. This extension
    isolates the erroneous larger upper bound, leaving all matching, persistence,
    and rejection behavior unchanged.
    """

    def _on_append_entries(self, src: int, request: AppendEntries) -> None:
        super()._on_append_entries(src, request)
        matched = request.prev_log_index + len(request.entries)
        accepted = (
            request.term == self.current_term
            and matched < len(self.log)
            and self.log[request.prev_log_index].term == request.prev_log_term
            and tuple(self.log[request.prev_log_index + 1:matched + 1]) == request.entries
        )
        if accepted:
            beyond_match = min(request.leader_commit, self.last_log_index)
            if beyond_match > self.commit_index:
                self._set_commit(beyond_match)


class _SnapshotStorage:
    """Plain durable snapshots used identically in both DoubleVote arms.

    Production MemoryStorage has a redundant same-term vote guard. Removing
    that guard in this isolated fixture lets the independent monitor, rather
    than storage, detect the protocol defect. Snapshot schema and term checks
    remain; all other scenarios use the unchanged production MemoryStorage.
    """

    def __init__(self, initial: PersistentState | None = None) -> None:
        self._state = initial or PersistentState()

    def load(self) -> PersistentState:
        return self._state

    def save(self, state: PersistentState) -> None:
        if not isinstance(state, PersistentState) or state.current_term < self._state.current_term:
            raise ValueError("invalid snapshot or term regression")
        self._state = state


class _MonitorViolation(Exception):
    """Wrap only assertions originating in the unchanged production monitor."""

    def __init__(self, error: AssertionError) -> None:
        self.message = str(error)
        frames = traceback.extract_tb(error.__traceback__)
        frame = next(frame for frame in reversed(frames)
                     if frame.filename.replace("\\", "/").endswith("raftlight/simulator.py"))
        self.assertion = frame.line or ""
        self.location = f"raftlight/simulator.py:{frame.lineno}"
        if "Leader Completeness" in self.message:
            self.invariant = "Leader Completeness"
        elif "Conflicting commitment" in self.message:
            self.invariant = "Committed-entry agreement"
        elif "Persisted vote changed or cleared" in self.message or "Double vote" in self.message:
            self.invariant = "Persisted-vote consistency"
        elif "node.voted_for == state.voted_for" in self.assertion:
            self.invariant = "Durable/volatile state agreement"
        else:
            self.invariant = self.message or self.assertion
        super().__init__(self.message)

    def as_dict(self) -> dict:
        return dict(invariant=self.invariant, message=self.message,
                    assertion=self.assertion, location=self.location)


class _ScheduledCluster(Cluster):
    """Actual core/outbox transitions, with an explicit reproducible schedule."""

    def __init__(self, size: int, seed: int, node_type: type[Node], target: int,
                 initial: dict[int, PersistentState] | None = None,
                 plain_vote_store: bool = False) -> None:
        self.node_type = node_type
        self.target = target
        self.initial = initial or {}
        self.plain_vote_store = plain_vote_store
        self._constructed: set[int] = set()
        self.trace: list[dict] = []
        super().__init__(size=size, seed=seed)
        self.trace.append(dict(action="initial_fixture", size=size, seed=seed,
                               nodes=self.snapshot()["nodes"],
                               storage="plain snapshot at target" if plain_vote_store else "MemoryStorage"))

    def _new_node(self, node_id: int) -> None:
        if node_id not in self._constructed:
            store_type = _SnapshotStorage if self.plain_vote_store and node_id == self.target else MemoryStorage
            self.stores[node_id] = store_type(self.initial.get(node_id))
            self._constructed.add(node_id)
        cls = self.node_type if node_id == self.target else Node
        self.nodes[node_id] = cls(
            node_id, [i for i in self.stores if i != node_id], self.stores[node_id],
            random.Random(self.rng.getrandbits(64)),
            lambda dst, message: self._send(node_id, dst, message), self._observe, now=self.now)

    def _observe(self, event: str, node: Node, info: dict) -> None:
        try:
            super()._observe(event, node, info)
        except AssertionError as error:
            raise _MonitorViolation(error) from error

    def assert_safety(self) -> None:
        try:
            super().assert_safety()
        except AssertionError as error:
            raise _MonitorViolation(error) from error

    def perform(self, action: str, operation: Callable[[], object], **details) -> object:
        self.now += .001
        self.trace.append(dict(step=len(self.trace), time=self.now, action=action, **details))
        try:
            result = operation()
            self.assert_safety()
            return result
        except _MonitorViolation as error:
            self.trace[-1]["first_monitor_detection"] = error.as_dict()
            raise

    def election(self, node_id: int) -> None:
        self.perform("start_election", lambda: self.nodes[node_id].start_election(self.now), node=node_id)

    def deliver(self, src: int, dst: int, kind: type,
                predicate: Callable[[Message], bool] = lambda _: True) -> Message:
        envelope = next((entry for entry in self.queue
                         if entry.src == src and entry.dst == dst
                         and isinstance(entry.message, kind) and predicate(entry.message)), None)
        if envelope is None:
            raise RuntimeError(f"scenario expected an emitted {kind.__name__} from {src} to {dst}")

        def delivery() -> None:
            self.queue.remove(envelope)
            self._deliver(envelope)

        self.perform("deliver_emitted_rpc", delivery, src=src, dst=dst,
                     message=message_to_dict(envelope.message))
        return envelope.message

    def vote(self, candidate: int, voter: int) -> None:
        term = self.nodes[candidate].current_term
        self.deliver(candidate, voter, RequestVote, lambda message: message.term == term)
        self.deliver(voter, candidate, VoteResponse, lambda message: message.term == term)

    def append(self, leader: int, follower: int, end: int | None = None,
               entries: int | None = None) -> AppendEntries:
        request = self.deliver(
            leader, follower, AppendEntries,
            lambda message: message.term == self.nodes[leader].current_term
            and (end is None or message.prev_log_index + len(message.entries) == end)
            and (entries is None or len(message.entries) == entries))
        self.deliver(follower, leader, AppendResponse,
                     lambda message: message.request_id == request.request_id)
        return request


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError("sensitivity fixture error: " + message)


def _no_election_restriction(cluster: _ScheduledCluster) -> None:
    cluster.election(1)
    cluster.vote(1, 2)
    cluster.append(1, 2, end=1, entries=1)
    cluster.append(1, 3, end=1, entries=1)
    cluster.perform("propose", lambda: cluster.nodes[1].propose(
        Command("set", "must-survive", "committed"), cluster.now), node=1)
    cluster.append(1, 2, end=2, entries=1)
    _require(cluster.nodes[1].commit_index == 2, "old leader must commit its client write")
    cluster.perform("crash", lambda: cluster.crash(1), node=1)
    cluster.election(3)
    cluster.vote(3, 2)
    _require(cluster.nodes[3].role == Role.CANDIDATE, "correct stale candidate must not win")


def _double_vote(cluster: _ScheduledCluster) -> None:
    cluster.election(1)
    cluster.election(2)
    cluster.vote(1, 3)
    cluster.vote(2, 3)
    _require(cluster.nodes[2].role == Role.CANDIDATE, "correct voter must deny its second candidate")


def _forget_vote(cluster: _ScheduledCluster) -> None:
    cluster.election(1)
    cluster.vote(1, 3)
    cluster.perform("crash", lambda: cluster.crash(3), node=3)
    cluster.perform("restart", lambda: cluster.restart(3), node=3)
    cluster.election(2)
    cluster.vote(2, 3)
    _require(cluster.nodes[3].voted_for == 1, "correct restart must retain the first vote")


def _branch(term: int, label: str, length: int = 8) -> tuple[LogEntry, ...]:
    return (LogEntry(0, Command("noop")), LogEntry(term, Command("noop")), *(
        LogEntry(term, Command("set", f"branch-{i}", f"{label}-{i}")) for i in range(2, length + 1)))


def _figure8_fixture() -> dict[int, PersistentState]:
    # Reachable uncommitted minority branches: node1 led term2; node5 then won
    # term3 with nodes2/3, but its replication was withheld. Its durable self-vote
    # and the other term3 grants are retained in the initial snapshots.
    empty = PersistentState().log
    return {1: PersistentState(3, None, _branch(2, "old")),
            2: PersistentState(3, 5, empty),
            3: PersistentState(3, 5, empty),
            4: PersistentState(3, 5, empty),
            5: PersistentState(3, 5, _branch(3, "conflicting"))}


def _naive_commit(cluster: _ScheduledCluster) -> None:
    cluster.election(1)  # Term4; auto-noop is index9, beyond the first batch.
    cluster.vote(1, 2)
    cluster.vote(1, 3)
    for follower in (2, 3):
        cluster.append(1, follower, end=9, entries=1)  # Missing prev8 -> reject.
        cluster.append(1, follower, end=8, entries=8)  # Actual older-term batch.
    cluster.trace.append(dict(action="withhold_current_term_noop", index=9,
                               old_prefix_replicas=[1, 2, 3],
                               observed_commit_index=cluster.nodes[1].commit_index))
    cluster.perform("crash", lambda: cluster.crash(1), node=1)
    # The old candidate's in-flight term4 request updates node5's term but is
    # denied because node5 has a newer last log term. Its next election is term5.
    cluster.deliver(1, 5, RequestVote, lambda message: message.term == 4)
    cluster.election(5)
    cluster.vote(5, 2)
    cluster.vote(5, 3)
    _require(cluster.nodes[5].role == Role.LEADER, "later correct leader must win term5")
    _require(not cluster.monitor.committed, "correct old-term prefix must remain uncommitted")


def _beyond_match_fixture() -> dict[int, PersistentState]:
    common = _branch(1, "common")
    old = common + (LogEntry(2, Command("noop")),
                    LogEntry(2, Command("set", "tail", "obsolete")))
    new = common + (LogEntry(3, Command("noop")),
                    LogEntry(3, Command("set", "tail", "committed")))
    return {1: PersistentState(3, None, old),
            2: PersistentState(3, 2, new),
            3: PersistentState(3, 2, new)}


def _commit_beyond_match(cluster: _ScheduledCluster) -> None:
    cluster.election(2)
    cluster.vote(2, 3)
    cluster.append(2, 3, end=11, entries=1)
    _require(cluster.nodes[2].commit_index == 11, "correct branch must be committed first")

    def conservative_retry() -> None:
        # This explicit volatile fixture conservatively underestimates follower
        # progress. It is identical in both arms, preserves match=0, and invokes
        # the actual batching code rather than constructing a synthetic RPC.
        cluster.nodes[2].next_index[1] = 1
        cluster.nodes[2]._send_append(1)

    cluster.perform("conservative_retry_fixture", conservative_retry,
                     leader=2, follower=1, next_index=1, match_index=0)
    request = cluster.append(2, 1, end=8, entries=8)
    _require(request.leader_commit == 11, "batch must advertise commits beyond verified index8")
    _require(cluster.nodes[1].commit_index == 8, "correct follower must stop at verified prefix")
    # Continue the control through ordinary suffix repair and convergence.
    cluster.append(2, 1, end=11, entries=3)
    _require(cluster.nodes[1].machine["tail"] == "committed", "correct follower must repair its tail")


def _restart_witness(cls: type[Node]) -> dict:
    """Behavioral continuation only; independent from the earliest detection.

    The main monitored experiment stops on its first violation. This separate
    run has no observer, deliberately allowing the defect to reach a real node
    reconstruction. It is never counted as an additional monitor detection.
    """
    storage = MemoryStorage()
    sent: list[Message] = []
    node = cls(3, [1, 2], storage, random.Random(304), lambda _, message: sent.append(message))
    node.receive(1, RequestVote(1, 1, 0, 0), .001)
    first_vote = sent[-1].vote_granted
    before = storage.load()
    memory_vote = node.voted_for
    del node  # Simulated crash: retain storage, discard the consensus instance.
    restored = cls(3, [1, 2], storage, random.Random(305),
                   lambda _, message: sent.append(message), now=.002)
    after = storage.load()
    restored.receive(2, RequestVote(1, 2, 0, 0), .003)
    return dict(crash_restart_executed=True,
                monitoring="behavioral continuation only; no observer; not a detection count",
                first_vote_granted=first_vote, second_vote_granted=sent[-1].vote_granted,
                pre_crash_memory_vote=memory_vote, pre_crash_durable_vote=before.voted_for,
                restart_vote=after.voted_for, term_preserved=before.current_term == after.current_term,
                log_preserved=before.log == after.log)


@dataclass(frozen=True)
class _Case:
    mutant: type[Node]
    defect: str
    seed: int
    size: int
    target: int
    scenario: str
    schedule: Callable[[_ScheduledCluster], None]
    fixture: Callable[[], dict[int, PersistentState]] | None = None
    plain_vote_store: bool = False
    notes: str = ""


_CASES = (
    _Case(NoElectionRestriction, "Omit candidate log-freshness restriction", 301, 3, 2,
          "A stale candidate seeks a vote after a client write commits on the old majority",
          _no_election_restriction),
    _Case(DoubleVote, "Grant a fresh candidate a second vote in the same term", 302, 3, 3,
          "Two simultaneous candidates request the same deciding follower's vote",
          _double_vote, plain_vote_store=True,
          notes="Both arms use plain snapshot storage at the voter. Production MemoryStorage also rejects changing a same-term vote; this fixture isolates monitor sensitivity. The monitor stops before a second election."),
    _Case(ForgetVoteOnRestart, "Acknowledge an external vote without its durable write", 303, 3, 3,
          "Grant a vote, crash and reconstruct the voter, then request a vote for the other candidate",
          _forget_vote,
          notes="The first monitor assertion is durable/volatile vote disagreement before the scheduled crash. A separately recorded, unmonitored behavioral continuation reconstructs the node and demonstrates the second vote; it is not counted as another monitor detection."),
    _Case(NaiveCommit, "Count an older-term replicated quorum without a current-term entry", 304, 5, 1,
          "Figure8-style older prefix reaches three replicas while its term4 no-op is withheld; a conflicting term3 branch wins term5",
          _naive_commit, _figure8_fixture,
          notes="Initial snapshots encode reachable uncommitted minority branches and their durable term3 votes. All election and replication replies come from actual core-generated RPCs; batching and automatic no-ops remain unchanged."),
    _Case(CommitBeyondMatch, "Cap follower commitment by local log end instead of the RPC-verified prefix", 305, 3, 1,
          "An actual eight-entry retry batch matches a common prefix while an unverified divergent tail remains",
          _commit_beyond_match, _beyond_match_fixture,
          notes="Both arms explicitly set a conservative next_index=1, match_index=0 retry fixture. The unchanged production sender emits the eight-entry RPC with leader_commit=11. The first assertion is conflicting commitment, before conflicting state-machine application."),
)


def _run_arm(case: _Case, mutant: bool) -> tuple[_ScheduledCluster, _MonitorViolation | None]:
    cluster = _ScheduledCluster(case.size, case.seed, case.mutant if mutant else Node,
                                case.target, case.fixture() if case.fixture else None,
                                case.plain_vote_store)
    try:
        case.schedule(cluster)
    except _MonitorViolation as detection:
        return cluster, detection
    return cluster, None


def run_sensitivity() -> dict:
    """Run exactly five correct controls and five independently injected defects."""
    experiments = []
    for case in _CASES:
        control, control_failure = _run_arm(case, mutant=False)
        mutated, detection = _run_arm(case, mutant=True)
        row = dict(mutant=case.mutant.__name__, defect=case.defect, seed=case.seed,
                   cluster_size=case.size, scenario=case.scenario,
                   correct_control_pass=control_failure is None, detected=detection is not None,
                   invariant=detection.invariant if detection else None,
                   message=detection.message if detection else None,
                   assertion=detection.assertion if detection else None,
                   location=detection.location if detection else None,
                   trace=mutated.trace, control_trace=control.trace,
                   first_detection_snapshot=mutated.snapshot() if detection else None,
                   control_failure=control_failure.as_dict() if control_failure else None,
                   notes=case.notes)
        if case.mutant is ForgetVoteOnRestart:
            row["restart_witness"] = dict(control=_restart_witness(Node),
                                           mutant=_restart_witness(ForgetVoteOnRestart))
        experiments.append(row)
    return dict(schema_version=1, experiment="five_protocol_mutant_sensitivity",
                methodology="Five fixed paired schedules; one faulty node per mutant arm; only assertions from the unchanged SafetyMonitor count as detections",
                mutants_total=len(experiments),
                detected_total=sum(row["detected"] for row in experiments),
                correct_controls_passed=sum(row["correct_control_pass"] for row in experiments),
                experiments=experiments)
