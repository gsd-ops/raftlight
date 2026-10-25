"""Log consistency, current-term commitment, and partition reconciliation."""
import pytest

from raftlight.messages import AppendEntries, AppendResponse, LogEntry, VoteResponse
from raftlight.node import Role
from raftlight.simulator import Cluster
from raftlight.state_machine import Command
from conftest import Harness, durable


def test_follower_truncates_only_conflicting_suffix():
    h = Harness()
    a = LogEntry(1, Command("set", "a", "A"))
    b = LogEntry(2, Command("set", "b", "wrong"))
    c = LogEntry(2, Command("set", "c", "wrong"))
    node, store = h.node(state=durable(2, entries=(a, b, c)))
    replacement = LogEntry(3, Command("set", "b", "right"))
    node.receive(2, AppendEntries(3, 2, 1, 1, (replacement,), 2, 8), 0.01)
    assert node.log[1:] == [a, replacement]
    assert node.machine == {"a": "A", "b": "right"}
    assert store.load().log == tuple(node.log)
    assert h.sent[-1][-1].success


def test_short_duplicate_append_preserves_matching_suffix_and_bounds_commit():
    h = Harness()
    entries = tuple(LogEntry(1, Command("set", str(i), str(i))) for i in range(1, 4))
    node, _ = h.node(state=durable(1, entries=entries))
    message = AppendEntries(1, 2, 0, 0, entries[:1], 3, 1)
    node.receive(2, message, 0.01)
    node.receive(2, message, 0.02)
    assert node.log[1:] == list(entries)
    assert node.commit_index == 1, "Cannot commit beyond the prefix verified by this request"
    assert node.machine == {"1": "1"}


def test_inconsistent_previous_term_rejects_without_modifying_log():
    h = Harness()
    node, _ = h.node(state=durable(2, entries=(LogEntry(1, Command("set", "x", "1")),)))
    original = tuple(node.log)
    node.receive(2, AppendEntries(2, 2, 1, 2, (), 1, 1), 0.01)
    assert tuple(node.log) == original
    assert not h.sent[-1][-1].success
    assert node.commit_index == 0


def test_stale_leader_cannot_replace_log_or_postpone_election():
    h = Harness()
    node, _ = h.node(state=durable(4, entries=(LogEntry(3, Command("set", "x", "safe")),)))
    deadline = node.election_deadline
    original = tuple(node.log)
    node.receive(2, AppendEntries(2, 2, 0, 0, (LogEntry(2, Command("set", "x", "stale")),), 1), 0.01)
    assert tuple(node.log) == original
    assert node.election_deadline == deadline
    assert not h.sent[-1][-1].success


def test_figure8_old_term_majority_requires_current_term_entry():
    """Figure 8's forbidden inference, isolated from transport batching.

    A leader with an old-term entry on a majority may only commit after
    its current-term no-op (or another current-term entry) has a majority.
    Setting replication acknowledgements directly makes the forbidden
    intermediate state observable even when transport batches whole suffixes.
    """
    h = Harness()
    old = LogEntry(2, Command("set", "x", "old-term"))
    node, _ = h.node(peers=(2, 3, 4, 5), state=durable(3, entries=(old,)))
    node.start_election(0.0)
    node.receive(2, VoteResponse(4, True), 0.01)
    node.receive(3, VoteResponse(4, True), 0.02)
    assert node.role == Role.LEADER
    assert node.log[2].term == 4
    node.match_index[2] = node.match_index[3] = 1
    node._advance_commit()
    assert node.commit_index == 0
    assert node.machine == {}
    node.match_index[2] = node.match_index[3] = 2
    node._advance_commit()
    assert node.commit_index == 2
    assert node.machine == {"x": "old-term"}


def test_delayed_old_response_cannot_regress_replication_progress():
    h = Harness()
    node, _ = h.node()
    node.start_election(0.0)
    node.receive(2, VoteResponse(1, True), 0.01)
    messages = [args[-1] for args in h.sent if isinstance(args[-1], AppendEntries)]
    first = messages[-1]
    node.propose(Command("set", "x", "new"), 0.02)
    messages = [args[-1] for args in h.sent if isinstance(args[-1], AppendEntries)]
    latest = messages[-1]
    node.receive(3, AppendResponse(1, True, len(node.log) - 1, request_id=latest.request_id), 0.03)
    progress = node.match_index[3]
    next_index = node.next_index[3]
    assert progress == node.last_log_index
    assert next_index == progress + 1
    node.receive(3, AppendResponse(1, False, conflict_index=1, request_id=first.request_id), 0.04)
    assert node.match_index[3] == progress
    assert node.next_index[3] >= next_index


@pytest.mark.parametrize("size", [3, 5])
def test_majority_commits_with_failed_follower(size):
    cluster = Cluster(size=size, seed=4)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    follower = next(i for i in cluster.alive if i != leader.node_id)
    cluster.crash(follower)
    proposal = cluster.submit(Command("set", "x", "10"))
    assert proposal is not None
    _, index = proposal
    assert cluster.wait_for(lambda: leader.commit_index >= index)
    assert leader.machine["x"] == "10"
    cluster.assert_safety()


def test_isolated_leader_cannot_commit_conflicting_write_and_heals():
    cluster = Cluster(size=5, seed=12)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    old_leader = cluster.leader()
    cluster.partition([[old_leader.node_id], [i for i in cluster.alive if i != old_leader.node_id]])
    committed_before = old_leader.commit_index
    old_index = old_leader.propose(Command("set", "x", "minority"), cluster.now)
    cluster.advance(1)
    assert old_leader.commit_index == committed_before
    assert old_index > old_leader.commit_index
    new_leader = cluster.leader()
    assert new_leader.node_id != old_leader.node_id
    _, new_index = cluster.submit(Command("set", "x", "majority"))
    assert cluster.wait_for(lambda: new_leader.commit_index >= new_index)
    assert cluster.stabilize(timeout=5)
    assert all(node.machine.get("x") == "majority" for node in cluster.nodes.values())
    assert all(Command("set", "x", "minority") not in [entry.command for entry in node.log]
               for node in cluster.nodes.values())
    cluster.assert_safety()


def test_set_delete_apply_in_log_order():
    cluster = Cluster(size=3, seed=5)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    for command in [Command("set", "x", "1"), Command("set", "y", "2"),
                    Command("set", "x", "3"), Command("delete", "y")]:
        proposal = cluster.submit(command)
        assert proposal is not None
        node_id, index = proposal
        assert cluster.wait_for(lambda: cluster.nodes[node_id].commit_index >= index)
    assert cluster.stabilize()
    assert all(node.machine == {"x": "3"} for node in cluster.nodes.values())
