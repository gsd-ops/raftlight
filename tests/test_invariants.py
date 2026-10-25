"""Shrinking adversarial event sequences against the actual consensus core."""
from __future__ import annotations

import json
from functools import wraps
from pathlib import Path

import pytest
from hypothesis import HealthCheck, note, settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from raftlight.messages import RequestVote, message_from_dict
from raftlight.simulator import Cluster
from raftlight.state_machine import Command
from conftest import Harness


def capture_counterexample(method):
    """Retain state even when a simulator observer raises inside a rule."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        if hasattr(self, "rule_events") and method.__name__ not in ("check", "teardown"):
            self.rule_events.append({"rule": method.__name__, "args": list(args),
                                     "kwargs": {key: sorted(value) if isinstance(value, set) else value
                                                for key, value in kwargs.items()}})
        try:
            return method(self, *args, **kwargs)
        except Exception:
            # Diagnostic capture followed by re-raise; no failure is suppressed.
            if hasattr(self, "sim"):
                self.record_failure()
            raise
    return wrapped


class RaftStateMachine(RuleBasedStateMachine):
    @initialize(size=st.sampled_from([3, 5]), seed=st.integers(0, 2**31 - 1))
    def cluster(self, size, seed):
        self.sim = Cluster(size=size, seed=seed)
        self.serial = 0
        self.rule_events = [{"rule": "initialize", "kwargs": {"size": size, "seed": seed}}]

    @rule(milliseconds=st.integers(0, 400))
    @capture_counterexample
    def tick_clock(self, milliseconds):
        self.sim.advance(milliseconds / 1000)

    @rule(index=st.integers(0, 100))
    @capture_counterexample
    def deliver_message(self, index):
        if self.sim.queue:
            self.sim.deliver_message(index % len(self.sim.queue))

    @rule(index=st.integers(0, 100))
    @capture_counterexample
    def drop_message(self, index):
        if self.sim.queue:
            self.sim.drop_message(index % len(self.sim.queue))

    @rule(index=st.integers(0, 100))
    @capture_counterexample
    def duplicate_message(self, index):
        if self.sim.queue:
            self.sim.duplicate_message(index % len(self.sim.queue))

    @rule(index=st.integers(0, 100), milliseconds=st.integers(1, 800))
    @capture_counterexample
    def delay_message(self, index, milliseconds):
        if self.sim.queue:
            self.sim.delay_message(index % len(self.sim.queue), milliseconds / 1000)

    @rule(group=st.sets(st.integers(1, 5), min_size=1, max_size=4))
    @capture_counterexample
    def partition_nodes(self, group):
        left = sorted(group.intersection(self.sim.nodes))
        right = sorted(set(self.sim.nodes) - set(left))
        if left and right:
            self.sim.partition([left, right])

    @rule(src=st.integers(1, 5), dst=st.integers(1, 5), enabled=st.booleans())
    @capture_counterexample
    def asymmetric_link(self, src, dst, enabled):
        if src in self.sim.nodes and dst in self.sim.nodes and src != dst:
            self.sim.set_link(src, dst, enabled)

    @rule()
    @capture_counterexample
    def heal_network(self):
        self.sim.heal()

    @rule(node_id=st.integers(1, 5))
    @capture_counterexample
    def crash_node(self, node_id):
        if node_id in self.sim.nodes:
            self.sim.crash(node_id)

    @rule(node_id=st.integers(1, 5))
    @capture_counterexample
    def restart_node(self, node_id):
        if node_id in self.sim.nodes:
            self.sim.restart(node_id)

    @rule(loss=st.sampled_from([0, .1, .2, .3, .4, .5, .6, .7, .8]))
    @capture_counterexample
    def packet_loss(self, loss):
        self.sim.loss = loss

    @rule(key=st.sampled_from(["x", "y", "z"]), delete=st.booleans())
    @capture_counterexample
    def submit_client_command(self, key, delete):
        self.serial += 1
        command = Command("delete", key) if delete else Command("set", key, str(self.serial))
        self.sim.submit(command)

    @rule()
    @capture_counterexample
    def simultaneous_elections(self):
        for node_id in sorted(self.sim.alive):
            self.sim.nodes[node_id].start_election(self.sim.now)

    def record_failure(self):
        self.failed = True
        snapshot = self.sim.snapshot()
        snapshot["generated_rule_sequence"] = self.rule_events
        note(json.dumps(snapshot, default=str, indent=2))
        path = Path("results/counterexamples")
        path.mkdir(parents=True, exist_ok=True)
        (path / "hypothesis_failure.json").write_text(
            json.dumps(snapshot, default=str, indent=2), encoding="utf-8")

    @capture_counterexample
    def check(self):
        self.sim.assert_safety()

    @invariant()
    def safety_after_every_event(self):
        self.check()

    @capture_counterexample
    def teardown(self):
        if hasattr(self, "sim") and not getattr(self, "failed", False):
            assert self.sim.stabilize(timeout=5), self.sim.snapshot()
            self.check()


TestRaftStateMachine = RaftStateMachine.TestCase
TestRaftStateMachine.settings = settings(
    max_examples=80, stateful_step_count=70, deadline=None,
    suppress_health_check=[HealthCheck.too_slow], derandomize=True,
)


@pytest.mark.parametrize("payload", [
    None, [], {}, {"type": "NotARaftMessage"},
    {"type": "RequestVote", "term": -1, "candidate_id": 2,
     "last_log_index": 0, "last_log_term": 0},
    {"type": "RequestVote", "term": True, "candidate_id": 2,
     "last_log_index": 0, "last_log_term": 0},
    {"type": "AppendEntries", "term": 1, "leader_id": 2,
     "prev_log_index": 0, "prev_log_term": 0, "entries": "invalid",
     "leader_commit": 0, "request_id": 1},
])
def test_malformed_rpc_rejected(payload):
    with pytest.raises(ValueError):
        message_from_dict(payload)


def test_unknown_sender_and_wrong_candidate_identity_cannot_change_state():
    h = Harness()
    node, store = h.node()
    before = store.load()
    with pytest.raises(ValueError):
        node.receive(99, RequestVote(100, 99, 0, 0), 0.1)
    assert store.load() == before
    assert h.sent == []
    with pytest.raises(ValueError):
        node.receive(2, RequestVote(100, 3, 0, 0), 0.2)
    assert store.load() == before
    assert h.sent == []
