"""Measure sequential committed writes through three independent localhost nodes.

Run from the repository root. Each node uses the normal CLI, its own asyncio
event loop, and an independent FileStorage file. Latencies use real monotonic
wall-clock time, not the simulator clock. No retries or warm-up writes are made.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import random
import socket
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from raftlight.state_machine import Command
from raftlight.storage import FileStorage
from raftlight.transport import request


def reserve_addresses() -> dict[int, tuple[str, int]]:
    sockets = [socket.socket() for _ in range(3)]
    try:
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return {i + 1: ("127.0.0.1", item.getsockname()[1])
                for i, item in enumerate(sockets)}
    finally:
        for item in sockets:
            item.close()


def percentile(values: list[float], fraction: float) -> float | None:
    """Linear interpolation between order statistics, as in the simulator plots."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (position - lower) * (ordered[upper] - ordered[lower])


def filesystem_name(path: Path) -> str | None:
    if os.name != "nt":
        return None
    import ctypes
    buffer = ctypes.create_unicode_buffer(256)
    success = ctypes.windll.kernel32.GetVolumeInformationW(
        str(path.anchor), None, 0, None, None, None, buffer, len(buffer))
    return buffer.value if success else None


async def statuses(addresses: dict[int, tuple[str, int]]) -> dict[int, dict[str, Any]]:
    result = {}
    for node_id, address in addresses.items():
        try:
            result[node_id] = await request(address, {"op": "status"}, timeout=0.5)
        except (OSError, ValueError, asyncio.TimeoutError):
            continue
    return result


async def measure(args: argparse.Namespace) -> dict[str, Any]:
    data_dir = args.data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=False)  # Never reuse or overwrite state.
    addresses = reserve_addresses()
    peer_text = ",".join(f"{node_id}={host}:{port}"
                         for node_id, (host, port) in addresses.items())
    seed_stream = random.Random(args.seed)
    node_seeds = {node_id: seed_stream.getrandbits(32) for node_id in addresses}
    processes: dict[int, subprocess.Popen[str]] = {}
    logs = {}
    observations = []
    commands = [{"op": "set", "key": f"tcp-{i:04d}", "value": f"value-{i:04d}"}
                for i in range(1, args.writes + 1)]
    expected = {command["key"]: command["value"] for command in commands}
    started = datetime.now(timezone.utc).isoformat()
    try:
        for node_id in addresses:
            logs[node_id] = (data_dir / f"node-{node_id}.log").open("w", encoding="utf-8")
            processes[node_id] = subprocess.Popen(
                [sys.executable, str(ROOT / "raft.py"), "node", "--id", str(node_id),
                 "--peers", peer_text, "--data", str(data_dir / f"node-{node_id}.json"),
                 "--seed", str(node_seeds[node_id])], cwd=ROOT,
                stdout=logs[node_id], stderr=subprocess.STDOUT, text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)

        deadline = time.monotonic() + 15
        leader_id = None
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes.values()):
                raise RuntimeError(f"node process exited; inspect {data_dir}")
            current = await statuses(addresses)
            leaders = [node_id for node_id, status in current.items()
                       if status.get("role") == "leader"]
            if (len(current) == 3 and len(leaders) == 1
                    and all(status["leader_id"] == leaders[0] for status in current.values())
                    and all(status["commit_index"] == status["last_applied"]
                            and status["last_applied"] >= 1 for status in current.values())):
                leader_id = leaders[0]
                break
            await asyncio.sleep(0.02)
        if leader_id is None:
            raise RuntimeError("three independent processes failed to elect and apply the leader no-op")

        measured_start_ns = time.perf_counter_ns()
        for sequence, command in enumerate(commands, 1):
            before = time.perf_counter_ns()
            try:
                response = await request(addresses[leader_id], command, timeout=5.0)
            except (OSError, ValueError, asyncio.TimeoutError) as error:
                response = {"ok": False, "error": type(error).__name__, "detail": str(error)}
            elapsed_ns = time.perf_counter_ns() - before
            observations.append({"sequence": sequence, "command": command,
                                 "elapsed_ns": elapsed_ns, "latency_ms": elapsed_ns / 1e6,
                                 "response": response})
        measured_elapsed_ns = time.perf_counter_ns() - measured_start_ns

        final = {}
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            final = await statuses(addresses)
            if (len(final) == 3
                    and all(status.get("state") == expected for status in final.values())
                    and all(status["last_applied"] == status["commit_index"]
                            and status["last_applied"] >= args.writes + 1
                            for status in final.values())):
                break
            await asyncio.sleep(0.02)

        durable = {}
        expected_commands = [Command(**command) for command in commands]
        for node_id in addresses:
            snapshot = FileStorage(data_dir / f"node-{node_id}.json").load()
            actual = [entry.command for entry in snapshot.log if entry.command.op == "set"]
            durable[node_id] = {"current_term": snapshot.current_term,
                                "last_log_index": len(snapshot.log) - 1,
                                "set_commands": len(actual),
                                "exact_commands_match": actual == expected_commands}

        successes = [row for row in observations if row["response"].get("ok") is True
                     and row["response"].get("committed") is True]
        latencies = [row["latency_ms"] for row in successes]
        all_applied = (len(final) == 3 and all(status.get("state") == expected
                       and status["last_applied"] == status["commit_index"]
                       and status["last_applied"] >= args.writes + 1 for status in final.values()))
        committed_indices = [row["response"]["index"] for row in successes]
        summary = {"offered": args.writes, "committed": len(successes),
                   "failed_or_unknown": args.writes - len(successes),
                   "p50_ms": percentile(latencies, 0.5),
                   "p95_ms": percentile(latencies, 0.95),
                   "p99_ms": percentile(latencies, 0.99),
                   "min_ms": min(latencies) if latencies else None,
                   "max_ms": max(latencies) if latencies else None,
                   "measured_elapsed_ms": measured_elapsed_ns / 1e6,
                   "all_nodes_applied_expected_state": all_applied,
                   "all_durable_command_sequences_match": all(item["exact_commands_match"]
                                                              for item in durable.values()),
                   "distinct_success_indices": len(set(committed_indices)) == len(successes),
                   "all_node_processes_alive": all(process.poll() is None
                                                   for process in processes.values())}
        return {"schema_version": 1, "started_utc": started,
                "method": {"nodes": 3, "node_processes": 3, "client_processes": 1,
                           "network": "IPv4 localhost TCP", "storage": "FileStorage",
                           "writes": "sequential SET commands with distinct keys and values",
                           "retries": 0, "warmup_writes": 0, "faults_injected": False,
                           "timing": "perf_counter_ns around await transport.request; includes connection establishment, send, committed reply and connection close",
                           "excluded_from_timing": "process startup, initial election, initial leader no-op, status polling and final convergence verification",
                           "clock_implementation": time.get_clock_info("perf_counter").implementation,
                           "clock_resolution_s": time.get_clock_info("perf_counter").resolution,
                           "quantiles": "linear interpolation between order statistics",
                           "base_seed": args.seed, "node_seeds": node_seeds,
                           "runtime_tick_interval_s": 0.01,
                           "fsync": "normal FileStorage full JSON snapshot, file flush/fsync and atomic replace; parent-directory fsync only on POSIX"},
                "environment": {"python": platform.python_version(),
                                "platform": platform.platform(), "machine": platform.machine(),
                                "logical_cpus": os.cpu_count(), "filesystem": filesystem_name(data_dir),
                                "storage_directory": (data_dir.resolve().relative_to(ROOT.resolve()).as_posix() if data_dir.resolve().is_relative_to(ROOT.resolve()) else data_dir.name),
                                "storage_device_type": "not identified; no NVMe/SSD claim",
                                "other_host_workload": "not controlled"},
                "summary": summary, "observations": observations,
                "final_node_statuses": final, "durable_snapshots": durable,
                "caveats": ["One local run; no independent-run confidence interval.",
                            "Loopback TCP does not model a physical network or multiple hosts.",
                            "The measurement does not isolate fsync, JSON or scheduler costs.",
                            "Wall-clock results vary with the host and workload."]}
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
        for process in processes.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for handle in logs.values():
            handle.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writes", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--data-dir", type=Path, required=True,
                        help="new directory for three durable node files and process logs")
    parser.add_argument("--output", type=Path, default=ROOT / "results" / "tcp_benchmark.json")
    args = parser.parse_args()
    if args.writes < 1:
        parser.error("--writes must be positive")
    record = asyncio.run(measure(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    summary = record["summary"]
    print(json.dumps(summary, indent=2))
    return 0 if (summary["committed"] == args.writes
                 and summary["all_nodes_applied_expected_state"]
                 and summary["all_durable_command_sequences_match"]
                 and summary["distinct_success_indices"]
                 and summary["all_node_processes_alive"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
