"""Crashes preserve durable state; eventual recovery rebuilds applied state."""
from raftlight.messages import LogEntry, RequestVote
from raftlight.node import Role
from raftlight.simulator import Cluster
from raftlight.state_machine import Command


def test_leader_crash_before_majority_does_not_promise_commit():
    cluster = Cluster(size=3, seed=20)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    cluster.partition([[leader.node_id], [i for i in cluster.alive if i != leader.node_id]])
    index = leader.propose(Command("set", "lost", "uncommitted"), cluster.now)
    assert leader.commit_index < index
    cluster.crash(leader.node_id)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    assert cluster.stabilize()
    assert all("lost" not in node.machine for node in cluster.nodes.values())


def test_leader_crash_after_commit_preserves_command():
    cluster = Cluster(size=5, seed=21)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    _, index = cluster.submit(Command("set", "durable", "yes"))
    assert cluster.wait_for(lambda: leader.commit_index >= index)
    cluster.crash(leader.node_id)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    assert cluster.stabilize()
    assert all(node.machine.get("durable") == "yes" for node in cluster.nodes.values())
    cluster.assert_safety()


def test_outdated_follower_restart_catches_up():
    cluster = Cluster(size=3, seed=22)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    follower_id = next(i for i in cluster.alive if i != leader.node_id)
    cluster.crash(follower_id)
    durable_before = cluster.stores[follower_id].load()
    # Exceed three replication batches; catching up must chain multiple RPCs.
    for i in range(25):
        _, index = cluster.submit(Command("set", str(i), str(i)))
        assert cluster.wait_for(lambda: leader.commit_index >= index)
    assert cluster.stores[follower_id].load() == durable_before
    cluster.restart(follower_id)
    assert cluster.nodes[follower_id].role == Role.FOLLOWER
    assert cluster.nodes[follower_id].last_applied == 0
    assert cluster.stabilize()
    assert cluster.nodes[follower_id].machine == leader.machine
    assert cluster.nodes[follower_id].log == leader.log


def test_full_cluster_restart_rebuilds_state_from_logs():
    cluster = Cluster(size=3, seed=23)
    assert cluster.wait_for(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    _, index = cluster.submit(Command("set", "persistent", "state"))
    assert cluster.wait_for(lambda: all(n.commit_index >= index for n in cluster.nodes.values()))
    before = {i: cluster.stores[i].load() for i in cluster.alive}
    for i in list(cluster.alive):
        cluster.crash(i)
    cluster.advance(0.5)
    assert not cluster.leaders
    for i in cluster.nodes:
        assert cluster.stores[i].load() == before[i]
        cluster.restart(i)
    assert cluster.stabilize(timeout=5)
    assert all(n.machine.get("persistent") == "state" for n in cluster.nodes.values())


def test_crashed_node_cannot_process_queued_messages():
    cluster = Cluster(size=3, seed=24)
    cluster.nodes[1].start_election(cluster.now)
    assert cluster.queue
    cluster.crash(2)
    before = cluster.stores[2].load()
    cluster.advance(0.1)
    assert cluster.stores[2].load() == before
    cluster.assert_safety()
