"""Election restrictions, split votes, stale information and durable votes."""
import pytest

from raftlight.messages import AppendEntries, LogEntry, RequestVote, VoteResponse
from raftlight.node import Role
from raftlight.simulator import Cluster
from raftlight.state_machine import Command
from conftest import Harness, durable


@pytest.mark.parametrize("size", [3, 5])
def test_election_and_progress(size):
    cluster = Cluster(size=size, seed=32)
    assert cluster.wait_for(lambda: cluster.leader() is not None, timeout=2)
    leader = cluster.leader()
    assert leader.role == Role.LEADER
    assert len(cluster.leaders) == 1
    cluster.assert_safety()


def test_simultaneous_candidates_split_then_retry():
    cluster = Cluster(size=3, seed=9)
    for node in cluster.nodes.values():
        node.start_election(cluster.now)
    cluster.advance(0.10)
    assert not cluster.leaders, "All three candidates already voted for themselves"
    assert cluster.wait_for(lambda: len(cluster.leaders) == 1, timeout=2)
    assert cluster.leader().current_term >= 2
    cluster.assert_safety()


@pytest.mark.parametrize("candidate_index,candidate_term,granted", [
    (100, 1, False), (1, 2, False), (2, 2, True), (1, 3, True),
])
def test_vote_compares_last_term_before_index(candidate_index, candidate_term, granted):
    h = Harness()
    node, store = h.node(state=durable(3, entries=(
        LogEntry(1, Command("set", "x", "1")),
        LogEntry(2, Command("set", "x", "2")),
    )))
    node.receive(2, RequestVote(4, 2, candidate_index, candidate_term), 0.1)
    response = h.sent[-1][-1]
    assert isinstance(response, VoteResponse)
    assert response.vote_granted is granted
    assert store.load().current_term == 4
    assert store.load().voted_for == (2 if granted else None)


def test_one_vote_survives_restart():
    h = Harness()
    node, store = h.node()
    node.receive(2, RequestVote(1, 2, 0, 0), 0.01)
    assert h.sent[-1][-1].vote_granted
    restored, _ = h.node(state=store.load(), now=0.02)
    restored.receive(3, RequestVote(1, 3, 0, 0), 0.03)
    assert not h.sent[-1][-1].vote_granted
    restored.receive(2, RequestVote(1, 2, 0, 0), 0.04)
    assert h.sent[-1][-1].vote_granted
    assert restored.voted_for == 2


def test_repeated_and_stale_votes_cannot_manufacture_quorum():
    h = Harness()
    node, _ = h.node(peers=(2, 3, 4, 5))
    node.start_election(0.0)
    node.receive(2, VoteResponse(node.current_term, True), 0.01)
    node.receive(2, VoteResponse(node.current_term, True), 0.02)
    node.receive(3, VoteResponse(node.current_term - 1, True), 0.03)
    assert node.role == Role.CANDIDATE
    node.receive(3, VoteResponse(node.current_term, True), 0.04)
    assert node.role == Role.LEADER


def test_higher_term_response_steps_leader_down():
    h = Harness()
    node, store = h.node()
    node.start_election(0.0)
    node.receive(2, VoteResponse(1, True), 0.01)
    assert node.role == Role.LEADER
    node.receive(3, VoteResponse(7, False), 0.02)
    assert node.role == Role.FOLLOWER
    assert store.load().current_term == 7
    assert store.load().voted_for is None


def test_same_term_append_steps_candidate_down():
    h = Harness()
    node, _ = h.node()
    node.start_election(0.0)
    node.receive(2, AppendEntries(1, 2, 0, 0, (), 0), 0.01)
    assert node.role == Role.FOLLOWER
    assert node.leader_id == 2


def test_repeated_isolated_elections_eventually_recover():
    cluster = Cluster(size=5, seed=14)
    cluster.partition([[1], [2], [3], [4], [5]])
    cluster.advance(2)
    assert not cluster.leaders
    assert all(node.current_term > 2 for node in cluster.nodes.values())
    assert cluster.stabilize(timeout=5)
    cluster.assert_safety()
