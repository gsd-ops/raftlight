"""Reproducible campaign: actual faults, historical safety, stable recovery.

Run from the repository root: python test_network_hell.py --faults 10000
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
import platform
import random
import time

from raftlight.messages import message_from_dict
from raftlight.simulator import Cluster
from raftlight.state_machine import Command


def inject(cluster: Cluster, rng: random.Random) -> str | None:
    """Return a category only when a concrete fault/repair was performed."""
    action = rng.randrange(11)
    ids = sorted(cluster.stores)
    index = rng.randrange(max(1, len(cluster.queue)))
    if action == 0 and cluster.alive:
        leader = cluster.leader()
        target = leader.node_id if leader and rng.random() < .6 else rng.choice(sorted(cluster.alive))
        role = cluster.nodes[target].role.value
        return role + "_crash" if cluster.crash(target) else None
    if action == 1:
        stopped = sorted(set(ids) - cluster.alive)
        return "restart" if stopped and cluster.restart(rng.choice(stopped)) else None
    if action == 2:
        return "message_drop" if cluster.drop_message(index) else None
    if action == 3:
        return "message_delay" if cluster.delay_message(index, rng.uniform(.1, .8)) else None
    if action == 4:
        return "message_duplicate" if cluster.duplicate_message(index) else None
    if action == 5:
        rng.shuffle(ids)
        split = rng.randrange(1, cluster.size)
        return "partition" if cluster.partition([ids[:split], ids[split:]]) else None
    if action == 6:
        return "partition_heal" if cluster.heal() else None
    if action == 7:
        src, dst = rng.sample(ids, 2)
        cluster.set_link(src, dst, not cluster.links[src, dst])
        return "directional_link_change"
    if action == 8 and cluster.queue:
        # Replay a real old-term packet, never synthesize a Byzantine sender.
        stale = [i for i, e in enumerate(cluster.queue)
                 if e.dst in cluster.alive and cluster.links[e.src, e.dst]
                 and e.message.term < cluster.nodes[e.dst].current_term]
        if stale:
            cluster.deliver_message(rng.choice(stale))
            return "stale_rpc_delivery"
    if action == 9 and cluster.queue:
        if len(cluster.queue) > 1:
            cluster.deliver_message(rng.randrange(1, len(cluster.queue)))
            return "message_reordering"
    if action == 10:
        # Invalid wire input must leave consensus and durable state untouched.
        before = {i: cluster._node_snapshot(n) for i, n in cluster.nodes.items()}
        invalid = {"type": "RequestVote", "term": -1, "candidate_id": ids[0]}
        try:
            message_from_dict(invalid)
        except (ValueError, TypeError):
            assert before == {i: cluster._node_snapshot(n) for i, n in cluster.nodes.items()}
            cluster._record("malformed_rejection", message=invalid)
            return "malformed_rejection"
        raise AssertionError("Malformed packet was accepted")
    return None


def run_trial(size: int, seed: int, faults: int, results: Path) -> dict:
    start = time.perf_counter()
    cluster = Cluster(size, seed)
    rng = random.Random(seed ^ 0xCA05)
    counts: Counter = Counter()
    operations: list[dict] = []
    cycles = 0
    cluster.advance(.5)
    try:
        while sum(counts.values()) < faults:
            category = inject(cluster, rng)
            if category:
                counts[category] += 1
            if cycles % 4 == 0:
                command = Command("set", f"key{cycles % 19}", f"{seed}:{cycles}")
                ticket = cluster.submit(command)
                operations.append(dict(ticket=ticket, command=asdict(command),
                                       submitted=cluster.now, deadline=cluster.now + .75,
                                       status="pending" if ticket else "rejected"))
            cluster.advance(rng.uniform(.005, .045))
            for operation in operations:
                if operation["status"] == "pending":
                    if (cluster.is_committed(operation["ticket"], Command(**operation["command"]))
                            and cluster.monitor.commit_times[operation["ticket"][1]] <= operation["deadline"]):
                        operation["status"] = "committed"
                    elif cluster.now >= operation["deadline"]:
                        operation["status"] = "timed_out"
            cycles += 1
            # Restore progress periodically, but count only effective repair events.
            if cycles % 60 == 0:
                if sum(counts.values()) < faults and cluster.heal():
                    counts["partition_heal"] += 1
                for node_id in sorted(cluster.stores):
                    if sum(counts.values()) < faults and cluster.restart(node_id):
                        counts["restart"] += 1
                cluster.advance(.5)
            cluster.assert_safety()
        pre_recovery = cluster.now
        recovered = cluster.stabilize(5)
        assert recovered, "Stable-network recovery did not converge within 5 simulated seconds"
        for operation in operations:
            if operation["status"] == "pending":
                operation["status"] = "committed" if (cluster.is_committed(
                    operation["ticket"], Command(**operation["command"])) and
                    cluster.monitor.commit_times[operation["ticket"][1]] <= operation["deadline"]) else "timed_out"
        cluster.assert_safety()
    except (AssertionError, ValueError) as error:
        failure = cluster.snapshot()
        failure.update(error=str(error), faults=dict(counts), cycles=cycles)
        (results / f"failure-{size}-{seed}.json").write_text(json.dumps(failure, indent=2), encoding="utf8")
        raise
    history = cluster.monitor.leadership_history
    leader_changes = sum(a["node"] != b["node"] for a, b in zip(history, history[1:]))
    statuses = Counter(o["status"] for o in operations)
    trial = dict(seed=seed, cluster_size=size, executed_events=len(cluster.events),
                 scheduler_cycles=cycles, injected_faults=sum(counts.values()), faults=dict(counts),
                 elections=cluster.stats["elections"], election_attempts=cluster.stats["election_attempts"],
                 leader_changes=leader_changes, committed_entries=len(cluster.monitor.committed),
                 committed_commands=sum(e.command.op != "noop" for e in cluster.monitor.committed.values()),
                 client_operations=len(operations), client_outcomes=dict(statuses),
                 failed_or_timed_out_operations=statuses["rejected"] + statuses["timed_out"],
                 safety_checks=dict(cluster.monitor.checks),
                 checked_safety_properties=sum(cluster.monitor.checks.values()), safety_violations=[],
                 simulation_duration_seconds=cluster.now,
                 stable_recovery_seconds=cluster.now - pre_recovery,
                 stable_recovery_passed=recovered, wall_clock_seconds=time.perf_counter() - start,
                 rpc_counts=dict(cluster.stats))
    trace = dict(seed=seed, cluster_size=size, events=cluster.events,
                 recent_rpc_history=list(cluster.rpc_history), client_operations=operations,
                 final_state=cluster.snapshot()["nodes"])
    (results / f"chaos-trace-{size}-{seed}.json").write_text(json.dumps(trace, indent=2), encoding="utf8")
    return trial


def campaign(faults: int = 10000, seed: int = 20261022, trials: int = 20,
             output: Path = Path("results/chaos_summary.json")) -> dict:
    if faults < 1 or trials < 1 or faults < trials:
        raise ValueError("Require faults >= trials >= 1")
    output.parent.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    runs = []
    for i in range(trials):
        count = faults // trials + int(i < faults % trials)
        run = run_trial(3 if i % 2 == 0 else 5, seed + i, count, output.parent)
        runs.append(run)
        print(f"Trial {i + 1}/{trials}: size={run['cluster_size']}, faults={count}, "
              f"checks={run['checked_safety_properties']}, recovered={run['stable_recovery_passed']}", flush=True)
    distribution: Counter = Counter()
    properties: Counter = Counter()
    outcomes: Counter = Counter()
    for run in runs:
        distribution.update(run["faults"])
        properties.update(run["safety_checks"])
        outcomes.update(run["client_outcomes"])
    summary = dict(schema_version=1, base_seed=seed, seeds=[r["seed"] for r in runs],
                   trials=runs, injected_faults=sum(distribution.values()), faults=dict(distribution),
                   executed_events=sum(r["executed_events"] for r in runs),
                   elections=sum(r["elections"] for r in runs),
                   leader_changes=sum(r["leader_changes"] for r in runs),
                   committed_entries=sum(r["committed_entries"] for r in runs),
                   committed_commands=sum(r["committed_commands"] for r in runs),
                   failed_or_timed_out_operations=sum(r["failed_or_timed_out_operations"] for r in runs),
                   client_outcomes=dict(outcomes), checked_safety_properties=sum(properties.values()),
                   safety_checks=dict(properties), safety_violations=[],
                   simulation_duration_seconds=sum(r["simulation_duration_seconds"] for r in runs),
                   wall_clock_seconds=time.perf_counter() - start,
                   environment=dict(python=platform.python_version(), platform=platform.platform()),
                   conditions=dict(election_timeout_seconds=[.15, .30], heartbeat_seconds=.05,
                                   timer_poll_seconds=.005, message_delay_seconds=[.001, .010],
                                   client_deadline_seconds=.75, stable_recovery_deadline_seconds=5,
                                   fault_selection="11 uniform choices; inapplicable actions not counted",
                                   recovery="every 60 cycles; effective repairs counted until target; final recovery excluded",
                                   client_measurement="global observed commit; no client response network model",
                                   failure_replay="same code and Python, --seed BASE --trials N --faults COUNT"))
    output.write_text(json.dumps(summary, indent=2), encoding="utf8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--faults", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=20261022)
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("results/chaos_summary.json"))
    args = parser.parse_args()
    result = campaign(args.faults, args.seed, args.trials, args.output)
    print(json.dumps({k: result[k] for k in ("injected_faults", "checked_safety_properties", "safety_violations")}, indent=2))
