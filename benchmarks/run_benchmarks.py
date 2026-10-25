"""Reproducible discrete-event experiments; all latencies are simulated time.

Run from the repository root:
python benchmarks/run_benchmarks.py --trials 100 --writes 5 --election-trials 200 --recovery-trials 20
No illustrative or invented observations are mixed into the output data.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from raftlight.simulator import Cluster
from raftlight.state_machine import Command


def quantiles(values: list[float]) -> dict:
    """Unsmoothed sample percentiles, conditional on observed commitment."""
    if not values:
        return {"p50_ms": None, "p95_ms": None, "p99_ms": None, "samples": 0}
    samples = np.asarray(values, dtype=float) * 1000
    return dict(zip(("p50_ms", "p95_ms", "p99_ms"),
                    (float(x) for x in np.percentile(samples, [50, 95, 99], method="linear"))), samples=len(values))


def latency_intervals(rows: list[dict], seed: int, repetitions: int = 2000) -> dict:
    """Pointwise percentile intervals resampling independent seeds as blocks.

    All offered operations for a sampled seed travel together, including failed
    operations. Successful latency remains conditional on commitment; its small
    tail sample and deadline censoring are not repaired by bootstrapping.
    """
    grouped = {}
    for row in rows:
        grouped.setdefault(row["seed"], []).append(row)
    if not grouped or repetitions < 1:
        raise ValueError("Bootstrap needs observed seed blocks and positive repetitions")
    blocks = list(grouped.values())
    rng = np.random.default_rng(seed)
    latency_samples, success_samples = [], []
    for _ in range(repetitions):
        resampled = [row for index in rng.integers(0, len(blocks), size=len(blocks))
                     for row in blocks[index]]
        successes = [row["latency_s"] * 1000 for row in resampled if row["outcome"] == "committed"]
        success_samples.append(100 * len(successes) / len(resampled))
        if successes:
            latency_samples.append(np.percentile(successes, [50, 95, 99], method="linear"))
    latency_bounds = np.percentile(latency_samples, [2.5, 97.5], axis=0, method="linear") if latency_samples else None
    return {"bootstrap_95ci_ms": {
                name: [float(latency_bounds[0, index]), float(latency_bounds[1, index])]
                if latency_bounds is not None else [None, None]
                for index, name in enumerate(("p50", "p95", "p99"))},
            "success_95ci_percent": [float(x) for x in np.percentile(success_samples, [2.5, 97.5], method="linear")],
            "bootstrap_seed": seed, "bootstrap_repetitions": repetitions,
            "bootstrap_nonempty_latency_resamples": len(latency_samples),
            "independent_seeds": len(blocks),
            "quantile_label": "Sample estimates conditional on commitment"}


def recognized_leader(cluster: Cluster) -> bool:
    leader = cluster.leader()
    return leader is not None and all(
        i == leader.node_id or (cluster.nodes[i].leader_id == leader.node_id
                               and cluster.nodes[i].current_term == leader.current_term)
        for i in cluster.alive)


def converged(cluster: Cluster) -> bool:
    if not cluster.alive:
        return False
    nodes = [cluster.nodes[i] for i in cluster.alive]
    first = nodes[0]
    return recognized_leader(cluster) and all(
        n.log == first.log and n.machine == first.machine
        and n.commit_index == first.commit_index == len(first.log) - 1
        for n in nodes)


def write_command(cluster: Cluster, key: str, value: str, timeout: float = 3.0) -> dict:
    command = Command("set", key, value)
    start = cluster.now
    ticket = cluster.submit(command)
    if ticket is None:
        return {"outcome": "no_leader", "latency_s": None, "elapsed_s": 0.0}
    success = cluster.wait_for(lambda: cluster.is_committed(ticket, command), timeout=timeout)
    return {"outcome": "committed" if success else "timeout", "ticket": list(ticket),
            "latency_s": cluster.now - start if success else None,
            "elapsed_s": cluster.now - start}


def election_metrics(cluster: Cluster, since=0.0) -> dict:
    attempts = [item for item in cluster.monitor.election_history if item["time"] >= since]
    counts = Counter(item["term"] for item in attempts)
    contested = [term for term, count in counts.items() if count > 1]
    split = [term for term in contested if term not in cluster.monitor.leaders_by_term]
    return {"election_attempts": len(attempts), "contested_terms": len(contested),
            "split_vote_terms": len(split), "split_vote_observed": bool(split),
            "attempt_history": attempts}


def experiment_latency(trials: int, writes: int) -> tuple[list[dict], list[dict]]:
    raw, summary = [], []
    for loss_percent in range(0, 81, 10):
        for trial in range(trials):
            seed = 10000 + trial
            cluster = Cluster(3, seed=seed, loss=loss_percent / 100)
            elected = cluster.wait_for(lambda: cluster.leader() is not None, timeout=5)
            for operation in range(writes):
                result = write_command(cluster, "value", f"{trial}:{operation}") if elected else {
                    "outcome": "no_leader", "latency_s": None, "elapsed_s": 0.0}
                raw.append({"loss_percent": loss_percent, "seed": seed, "size": 3,
                            "operation": operation, "initial_election_s": cluster.monitor.leadership_history[0]["time"]
                            if cluster.monitor.leadership_history else None,
                            "simulated_time_s": cluster.now, **result})
                # Election discovery between attempts is an explicit separate wait.
                elected = cluster.wait_for(lambda: cluster.leader() is not None, timeout=1)
            cluster.assert_safety()
        rows = [row for row in raw if row["loss_percent"] == loss_percent]
        successful = [row["latency_s"] for row in rows if row["outcome"] == "committed"]
        outcomes = Counter(row["outcome"] for row in rows)
        summary.append({"loss_percent": loss_percent, "attempts": len(rows),
                        "success_percent": len(successful) / len(rows) * 100,
                        "outcomes": dict(outcomes), **quantiles(successful),
                        **latency_intervals(rows, seed=40000 + loss_percent)})
        print(f"A loss={loss_percent}%: {len(successful)}/{len(rows)} committed", flush=True)
    return raw, summary


def experiment_elections(trials: int) -> tuple[list[dict], list[dict]]:
    raw, summary = [], []
    for size in (3, 5):
        for trial in range(trials):
            seed = 20000 + trial
            cluster = Cluster(size, seed=seed)
            success = cluster.wait_for(lambda: recognized_leader(cluster), timeout=3)
            initial = {"outcome": "converged" if success else "timeout",
                       "convergence_s": cluster.now if success else None,
                       "rpc_sent": cluster.stats["rpc_sent"], **election_metrics(cluster)}
            if not success:
                raw.append(dict(size=size, seed=seed, initial=initial, recovery=None))
                continue
            assert cluster.wait_for(lambda: converged(cluster), timeout=1), "Election baseline no-op did not converge"
            old_leader = cluster.leader()
            start = cluster.now
            before_rpc = cluster.stats["rpc_sent"]
            cluster.crash(old_leader.node_id)
            success = cluster.wait_for(lambda: recognized_leader(cluster), timeout=3)
            recovery = {"outcome": "converged" if success else "timeout",
                        "convergence_s": cluster.now - start if success else None,
                        "rpc_sent": cluster.stats["rpc_sent"] - before_rpc,
                        **election_metrics(cluster, since=start)}
            raw.append(dict(size=size, seed=seed, initial=initial, recovery=recovery))
        rows = [row for row in raw if row["size"] == size]
        item = {"size": size, "trials": len(rows)}
        for phase in ("initial", "recovery"):
            metrics = [row[phase] for row in rows if row[phase] is not None]
            item[phase] = {**quantiles([m["convergence_s"] for m in metrics if m["convergence_s"] is not None]),
                           "phase_trials": len(metrics),
                           "timeouts": sum(m["outcome"] == "timeout" for m in metrics),
                           "mean_election_attempts": float(np.mean([m["election_attempts"] for m in metrics])),
                           "mean_rpc_sent": float(np.mean([m["rpc_sent"] for m in metrics])),
                           "split_vote_trial_percent": 100 * sum(m["split_vote_observed"] for m in metrics) / len(metrics),
                           "split_vote_trials": sum(m["split_vote_observed"] for m in metrics),
                           "split_vote_terms": sum(m["split_vote_terms"] for m in metrics)}
        summary.append(item)
        print(f"B size={size}: {trials} initial and failure-recovery trials", flush=True)
    return raw, summary


def majority_write(cluster: Cluster, eligible: set[int], key: str) -> bool:
    if not cluster.wait_for(lambda: cluster.leader() is not None
                           and cluster.leader().node_id in eligible, timeout=3):
        return False
    return write_command(cluster, key, "committed", timeout=3)["outcome"] == "committed"


def experiment_recovery(trials: int) -> tuple[list[dict], list[dict]]:
    raw, summary = [], []
    scenarios = ("leader_crash", "minority_partition", "partition_healing", "outdated_follower_restart")
    for scenario in scenarios:
        for trial in range(trials):
            seed = 30000 + trial
            cluster = Cluster(5, seed=seed)
            assert cluster.stabilize(), "Stable baseline did not converge"
            assert write_command(cluster, "baseline", "1")["outcome"] == "committed"
            assert cluster.wait_for(lambda: converged(cluster), timeout=3)
            leader = cluster.leader()
            followers = [i for i in cluster.alive if i != leader.node_id]
            if scenario == "leader_crash":
                start = cluster.now
                cluster.crash(leader.node_id)
                success = majority_write(cluster, set(cluster.alive), scenario)
            elif scenario == "minority_partition":
                start = cluster.now
                majority = set(followers[1:])
                cluster.partition([[leader.node_id, followers[0]], sorted(majority)])
                success = majority_write(cluster, majority, scenario)
            elif scenario == "partition_healing":
                majority = set(followers[1:])
                cluster.partition([[leader.node_id, followers[0]], sorted(majority)])
                leader.propose(Command("set", "uncommitted", "minority"), cluster.now)
                assert majority_write(cluster, majority, "majority")
                start = cluster.now
                cluster.heal()
                success = cluster.wait_for(lambda: converged(cluster), timeout=3)
            else:
                lagging = followers[0]
                cluster.crash(lagging)
                for operation in range(8):
                    assert write_command(cluster, f"backlog-{operation}", str(operation))["outcome"] == "committed"
                start = cluster.now
                cluster.restart(lagging)
                success = cluster.wait_for(lambda: converged(cluster), timeout=3)
            raw.append({"scenario": scenario, "seed": seed, "size": 5,
                        "outcome": "recovered" if success else "timeout",
                        "recovery_s": cluster.now - start if success else None,
                        "elapsed_s": cluster.now - start, "rpc_sent_total": cluster.stats["rpc_sent"]})
            cluster.assert_safety()
        rows = [r for r in raw if r["scenario"] == scenario]
        summary.append({"scenario": scenario, "trials": len(rows),
                        "timeouts": sum(row["outcome"] == "timeout" for row in rows),
                        **quantiles([row["recovery_s"] for row in rows if row["recovery_s"] is not None])})
        print(f"C {scenario}: {trials} trials", flush=True)
    return raw, summary


def save_figure(fig, directory: Path, name: str, *, pad_inches: float = 0.15) -> None:
    # Use one layout engine, then retain a visible margin around every artist.
    fig.set_layout_engine("none")
    fig.tight_layout(pad=1.2)
    fig.savefig(directory / f"{name}.pdf", bbox_inches="tight", pad_inches=pad_inches)
    fig.savefig(directory / f"{name}.png", dpi=180, bbox_inches="tight", pad_inches=pad_inches)
    plt.close(fig)


def generate_figures(summary: dict, figures: Path, chaos_path: Path) -> None:
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "figure.dpi": 120})
    a = summary["latency"]
    x = [r["loss_percent"] for r in a]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), constrained_layout=True)
    for key, label in (("p50_ms", "p50"), ("p95_ms", "p95"), ("p99_ms", "p99 sample estimate")):
        axes[0].plot(x, [r[key] if r[key] is not None else np.nan for r in a], marker="o", label=label,
                     linestyle="--" if key == "p99_ms" else "-")
    if all("bootstrap_95ci_ms" in row for row in a):
        lower = [row["bootstrap_95ci_ms"]["p95"][0] for row in a]
        upper = [row["bootstrap_95ci_ms"]["p95"][1] for row in a]
        axes[0].fill_between(x, lower, upper, color="#ff7f0e", alpha=.16, label="p95: 95% seed-bootstrap CI")
    axes[0].set(xlabel="Independent packet loss (%)", ylabel="Successful commit latency (simulated ms)",
                title="A. Commit latency, 3 nodes", xticks=x)
    axes[0].legend(fontsize=8)
    axes[0].grid(alpha=.2)
    bottom = np.zeros(len(a))
    for outcome, label, color in (("committed", "Committed", "#3b8a60"),
                                  ("timeout", "Timeout", "#d89b2b"),
                                  ("no_leader", "No leader", "#c7524f")):
        percentages = np.array([100 * row["outcomes"].get(outcome, 0) / row["attempts"]
                                for row in a])
        axes[1].bar(x, percentages, width=6, bottom=bottom, color=color, label=label)
        bottom += percentages
    axes[1].set(xlabel="Independent packet loss (%)", ylabel="Offered attempts (%)", ylim=(0, 100),
                yticks=[0, 20, 40, 60, 80, 100], xticks=x)
    axes[1].set_title("B. Write outcomes, 3 nodes", pad=34)
    axes[1].legend(loc="lower center", bbox_to_anchor=(.5, 1.01), ncol=3,
                   fontsize=8, frameon=False, columnspacing=1.2, handlelength=1.4)
    axes[1].set_axisbelow(True)
    axes[1].grid(axis="y", alpha=.2)
    save_figure(fig, figures, "latency_vs_packet_loss")

    b = summary["elections"]
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.6), constrained_layout=True)
    positions = np.arange(len(b))
    for shift, phase, label, color in ((-.18, "initial", "Initial election", "#207e81"),
                                      (.18, "recovery", "After leader failure", "#c77831")):
        medians = [row[phase]["p50_ms"] for row in b]
        upper = [row[phase]["p95_ms"] - row[phase]["p50_ms"] for row in b]
        axes[0].bar(positions + shift, medians, .34, label=label, color=color,
                    yerr=[np.zeros(len(b)), upper], capsize=4)
        axes[1].bar(positions + shift, [row[phase]["mean_rpc_sent"] for row in b], .34,
                    label=label, color=color)
        rates = [row[phase]["split_vote_trial_percent"] for row in b]
        bars = axes[2].bar(positions + shift, rates, .34, label=label, color=color)
        for bar, row in zip(bars, b):
            count = row[phase]["split_vote_trials"]
            denominator = row[phase]["phase_trials"]
            axes[2].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + .12,
                         f"{count}/{denominator}", ha="center", fontsize=8)
    for ax in axes:
        ax.set_xticks(positions, [f"{row['size']} nodes" for row in b])
        ax.grid(axis="y", alpha=.2)
    axes[0].set(ylabel="Convergence (simulated ms)", title="B. Median; whisker = p95")
    axes[0].legend(fontsize=8)
    axes[1].set(ylabel="Mean sent RPCs", title="Requests + responses")
    maximum_rate = max(row[phase]["split_vote_trial_percent"] for row in b for phase in ("initial", "recovery"))
    axes[2].set(ylabel="Trials with a split vote (%)", ylim=(0, max(2, maximum_rate * 1.35)),
                title="Observed split-vote rate")
    save_figure(fig, figures, "election_convergence")

    c = summary["recovery"]
    fig, ax = plt.subplots(figsize=(8, 3.7), constrained_layout=True)
    medians = [row["p50_ms"] for row in c]
    ax.bar(np.arange(len(c)), medians, color=["#207e81", "#c77831", "#6077a4", "#828b54"],
           yerr=[np.zeros(len(c)), [row["p95_ms"] - row["p50_ms"] for row in c]], capsize=4)
    ax.set_xticks(np.arange(len(c)), ["Leader\ncrash", "Minority\npartition", "Partition\nhealing", "Outdated follower\nrestart"])
    ax.set(ylabel="Recovery (simulated ms)", title="C. Five-node recovery: median with p95 whisker")
    ax.grid(axis="y", alpha=.2)
    save_figure(fig, figures, "recovery_performance")

    if chaos_path.exists():
        chaos = json.loads(chaos_path.read_text(encoding="utf-8"))
        counts = chaos.get("faults", chaos.get("faults_by_category", chaos.get("fault_counts", {})))
        if not counts:
            raise ValueError("Chaos summary must include faults_by_category or fault_counts")
        fig, ax = plt.subplots(figsize=(8, max(3.5, len(counts) * .32)), constrained_layout=True)
        labels, numbers = zip(*sorted(counts.items(), key=lambda item: item[1]))
        colors = ["#828b54" if label in ("restart", "partition_heal") else
                  "#c77831" if label == "malformed_rejection" else "#207e81" for label in labels]
        ax.barh([label.replace("_", " ") for label in labels], numbers, color=colors)
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color="#207e81", label="Network / node action"),
                           Patch(color="#828b54", label="Recovery action"),
                           Patch(color="#c77831", label="Parser rejection check")], loc="lower right", fontsize=8)
        ax.set(xlabel="Injected Chaos & Recovery Actions", title=f"D. {sum(numbers):,} chaos/recovery actions")
        for index, count in enumerate(numbers):
            ax.text(count, index, f" {count:,}", va="center", fontsize=8)
        ax.set_xlim(0, max(numbers) * 1.15)
        # Extra whitespace protects the long action labels, title and legend border.
        save_figure(fig, figures, "chaos_summary", pad_inches=0.30)
    else:
        print(f"D skipped: run the chaos campaign first ({chaos_path})", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=100, help="Independent packet-loss seeds per condition (minimum 10)")
    parser.add_argument("--writes", type=int, default=5, help="Sequential offered operations per packet-loss seed")
    parser.add_argument("--election-trials", type=int, default=200, help="Independent election seeds per cluster size")
    parser.add_argument("--recovery-trials", type=int, default=20, help="Independent seeds per recovery scenario")
    parser.add_argument("--output", type=Path, default=ROOT / "results")
    parser.add_argument("--figures", type=Path, default=ROOT / "report/figures")
    parser.add_argument("--figures-only", action="store_true", help="Regenerate plots from existing measured JSON")
    args = parser.parse_args()
    if min(args.trials, args.election_trials, args.recovery_trials) < 10 or args.writes < 1:
        parser.error("Each experiment requires at least 10 trials and one offered write")
    args.output.mkdir(parents=True, exist_ok=True)
    args.figures.mkdir(parents=True, exist_ok=True)
    summary_path = args.output / "benchmark_summary.json"
    if args.figures_only:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    else:
        start = time.perf_counter()
        a_raw, a = experiment_latency(args.trials, args.writes)
        b_raw, b = experiment_elections(args.election_trials)
        c_raw, c = experiment_recovery(args.recovery_trials)
        methodology = {
            "time_basis": "simulated seconds; figure labels use simulated milliseconds",
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "real_execution_seconds": time.perf_counter() - start,
            "trials_per_condition": {"latency": args.trials, "elections": args.election_trials,
                                     "recovery": args.recovery_trials},
            "seeds": {"latency": [10000, 10000 + args.trials - 1],
                      "elections": [20000, 20000 + args.election_trials - 1],
                      "recovery": [30000, 30000 + args.recovery_trials - 1]},
            "statistics": {"quantiles": "Unsmoothed NumPy linear sample percentiles among committed operations.",
                           "bootstrap": "2000 independent-seed block resamples; all operations and failures within each seed retained. Pointwise 95% percentile intervals, conditional on nonempty successful samples for latency.",
                           "bootstrap_seeds": "40000 + packet-loss percentage",
                           "p99": "Descriptive sample estimate; offered attempt count is not successful sample count. The bootstrap cannot establish an unobserved tail or remove timeout censoring.",
                           "figure_interval": "Shading is the pointwise p95 latency 95% bootstrap interval; other interval values are preserved in JSON."},
            "network_delay_seconds": [.001, .010], "timer_resolution_seconds": .005,
            "election_timeout_seconds": [.150, .300], "heartbeat_interval_seconds": .050,
            "latency": {"cluster_size": 3, "loss_percent": list(range(0, 81, 10)),
                        "workload": f"{args.writes} sequential offered SET write attempts per seed per condition",
                        "startup_timeout_seconds": 5, "between_attempt_election_wait_seconds": 1,
                        "commit_timeout_seconds": 3,
                        "measurement": "Proposal to first observed commitment of the exact entry; success can be observed after leadership change. No client retries.",
                        "failures": "Timeout and no-leader outcomes are excluded from successful latency percentiles but counted in the success denominator.",
                        "loss_model": "Each sent request or response independently dropped; applies during elections and workload."},
            "elections": {"cluster_sizes": [3, 5], "loss": 0,
                          "measurement": "Startup/failure to every live node recognizing the highest-term leader; 3-second timeout.",
                          "rpc_count": "Sent protocol messages including responses and messages addressed to crashed nodes.",
                          "split_vote": "Term with at least two candidacies and no elected leader observed by convergence.",
                          "workload": "Election and initial no-op only; then crash the leader."},
            "recovery": {"cluster_size": 5, "loss": 0,
                         "measurement": "Crash/minority partition to new majority write committed; healing/restart to identical full logs, commit indexes and state machines.",
                         "timeouts": "Up to 3 seconds for leader discovery plus 3 seconds for commit; 3 seconds for full convergence.",
                         "partition": "Old leader plus one follower isolated from remaining three; minority write remains uncommitted.",
                         "restart_backlog": "Eight SET commands while one follower is crashed."},
            "limitations": ["Discrete-event measurements are not physical-network or Python throughput benchmarks.",
                            "Finite independent seeds; p99 estimates with small samples are descriptive, not precise tail guarantees.",
                            "Paired seed numbers across conditions do not imply identical random draws after fault paths diverge.",
                            "Successful latency is conditional on success and is subject to timeout censoring."]}
        summary = {"methodology": methodology, "latency": a, "elections": b, "recovery": c}
        raw = {"methodology": methodology, "latency": a_raw, "elections": b_raw, "recovery": c_raw}
        (args.output / "benchmark_raw.json").write_text(json.dumps(raw, indent=2), encoding="utf-8")
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    generate_figures(summary, args.figures, args.output / "chaos_summary.json")
    print(f"Saved observations to {args.output} and figures to {args.figures}")


if __name__ == "__main__":
    main()
