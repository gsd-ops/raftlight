"""Negative controls: prove the historical safety oracles detect injected bugs.

These tests deliberately bypass Node and storage protections to corrupt state.
They test the independent checker, not additional supported fault capabilities.
"""

import pytest

from raftlight.messages import LogEntry
from raftlight.node import Role
from raftlight.simulator import Cluster, SafetyMonitor
from raftlight.state_machine import Command, apply_command
from raftlight.storage import PersistentState


def committed_cluster():
    cluster = Cluster(3, seed=71)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    ticket = cluster.submit(Command("set", "committed", "original"))
    assert ticket is not None
    assert cluster.stabilize()
    return cluster, ticket[1]


def test_monitor_detects_committed_log_overwrite_after_restart():
    cluster, index = committed_cluster()
    victim = next(node for node in cluster.nodes if node != cluster.leader().node_id)
    cluster.crash(victim)
    cluster.restart(victim)
    node = cluster.nodes[victim]
    assert node.last_applied == node.commit_index == 0
    # Different terms avoid the pairwise Log Matching trigger. No new commit or
    # election is emitted, so only durable historical-prefix checking catches it.
    node.current_term += 1
    node.voted_for = None
    node.log[index] = LogEntry(node.current_term, Command("set", "committed", "corrupt"))
    cluster.stores[victim]._state = PersistentState(node.current_term, None, tuple(node.log))
    with pytest.raises(AssertionError):
        cluster.assert_safety()


def test_monitor_detects_position_regression_in_same_incarnation():
    cluster, index = committed_cluster()
    node = cluster.leader()
    node.commit_index = node.last_applied = index - 1
    node.machine.clear()
    for entry in node.log[1:node.last_applied + 1]:
        apply_command(node.machine, entry.command)
    with pytest.raises(AssertionError):
        cluster.assert_safety()


def test_monitor_detects_same_term_persisted_vote_cleared():
    cluster = Cluster(3, seed=72)
    node = cluster.nodes[1]
    node.start_election(cluster.now)
    cluster.assert_safety()
    node.voted_for = None
    cluster.stores[1]._state = PersistentState(node.current_term, None, tuple(node.log))
    with pytest.raises(AssertionError):
        cluster.assert_safety()


def test_election_safety_history_survives_old_leader_crash():
    cluster = Cluster(3, seed=73)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    previous = cluster.leader()
    cluster.crash(previous.node_id)
    fake = next(node for node in cluster.nodes.values() if node.node_id != previous.node_id)
    fake.current_term = previous.current_term
    with pytest.raises(AssertionError, match="Election Safety"):
        cluster.monitor.observe("leader", fake, {}, cluster.now)


def test_state_machine_safety_history_survives_crash_and_restart():
    cluster, index = committed_cluster()
    victim = cluster.leader().node_id
    cluster.crash(victim)
    cluster.restart(victim)
    node = cluster.nodes[victim]
    entry = LogEntry(node.current_term, Command("set", "committed", "different"))
    node.log[index] = entry
    node.commit_index = index
    with pytest.raises(AssertionError, match="State Machine Safety"):
        cluster.monitor.observe("apply", node, {"index": index, "entry": entry}, cluster.now)


def test_log_matching_checks_commands_and_entire_prefix():
    cluster = Cluster(3, seed=74)
    left, right = cluster.nodes[1], cluster.nodes[2]
    shared = LogEntry(2, Command("noop"))
    for node, value in [(left, "a"), (right, "b")]:
        node.current_term = 2
        node.log += [LogEntry(1, Command("set", "x", value)), shared]
        cluster.stores[node.node_id]._state = PersistentState(2, None, tuple(node.log))
    with pytest.raises(AssertionError, match="Log Matching"):
        cluster.assert_safety()


def test_late_commit_observation_checks_already_recorded_newer_leader():
    monitor = SafetyMonitor()
    cluster = Cluster(3, seed=75)
    earlier, later = cluster.nodes[1], cluster.nodes[2]
    earlier.current_term = 1
    earlier.role = Role.LEADER
    entry = LogEntry(1, Command("set", "x", "must survive"))
    earlier.log.append(entry)
    earlier.commit_index = 1
    later.current_term = 2
    later.role = Role.LEADER
    monitor.observe("leader", later, {}, 1.0)
    with pytest.raises(AssertionError, match="Leader Completeness"):
        monitor.observe("commit", earlier, {"index": 1, "entry": entry}, 2.0)
