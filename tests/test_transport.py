"""Wall-clock TCP integration tests, including independent OS processes."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import random
import socket
import subprocess
import sys
import time
from typing import Callable, Any

import pytest

from raftlight.messages import AppendEntries, AppendResponse, LogEntry, VoteResponse
from raftlight.node import Node, Role
from raftlight.state_machine import Command
from raftlight.storage import MemoryStorage, StorageFailure
from raftlight.transport import Runtime, parse_address, parse_peers, request


def addresses(count: int) -> dict[int, tuple[str, int]]:
    """Reserve distinct ephemeral ports together, then release for the listeners."""
    sockets = [socket.socket() for _ in range(count)]
    try:
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return {i + 1: ("127.0.0.1", item.getsockname()[1]) for i, item in enumerate(sockets)}
    finally:
        for item in sockets:
            item.close()


async def eventually(predicate: Callable[[], Any], *, timeout: float = 15.0) -> Any:
    """Bounded polling is appropriate here: these tests exercise physical TCP time."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.01)
    raise AssertionError("TCP runtime condition did not converge within deadline")


def shared_runtime(
    node_id: int,
    members: dict[int, tuple[str, int]],
    data_path: Path,
    *,
    seed: int,
    client_timeout: float = 5.0,
) -> Runtime:
    """Create an in-process Runtime sized for multi-node single-loop FileStorage tests.

    Production runs one Runtime per OS process; multiplexing 3-5 FileStorage nodes
    onto one asyncio loop serializes every fsync on a single thread, which can take
    hundreds of milliseconds on virtualized Windows CI runners.
    """
    return Runtime(
        node_id,
        members,
        data_path,
        seed=seed,
        client_timeout=client_timeout,
        election_timeout=(0.60, 1.10),
        heartbeat_interval=0.12,
    )


def leader_in(runtimes: dict[int, Runtime]) -> Runtime | None:
    for runtime in runtimes.values():
        if runtime.failure:
            raise runtime.failure
    leaders = [r for r in runtimes.values() if r.node and r.node.role == Role.LEADER]
    if len(leaders) != 1:
        return None
    leader = leaders[0]
    assert leader.node is not None
    if all(
        r.node and r.node.leader_id == leader.node_id and r.node.current_term == leader.node.current_term
        for r in runtimes.values()
    ):
        return leader
    return None


async def commit_write(
    runtimes: dict[int, Runtime],
    members: dict[int, tuple[str, int]],
    payload: dict[str, Any],
    *,
    timeout: float = 15.0,
) -> tuple[Runtime, dict[str, Any]]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_response: dict[str, Any] | None = None
    while loop.time() < deadline:
        remaining = max(0.5, deadline - loop.time())
        leader = await eventually(lambda: leader_in(runtimes), timeout=remaining)
        response = await request(members[leader.node_id], payload)
        if response.get("ok") and response.get("committed"):
            return leader, response
        last_response = response
        if response.get("error") not in {"not_leader", "leadership_changed", "timeout"}:
            break
        await asyncio.sleep(0.02)
    raise AssertionError(f"client write did not commit within deadline: {last_response}")


@pytest.mark.parametrize("count", [3, 5])
def test_tcp_replication_failover_and_durable_restart(tmp_path: Path, count: int) -> None:
    async def scenario() -> None:
        members = addresses(count)
        live: dict[int, Runtime] = {}
        try:
            for node_id in members:
                live[node_id] = await shared_runtime(node_id, members, tmp_path / f"{node_id}.json", seed=node_id).start()
            leader, result = await commit_write(live, members, {"op": "set", "key": "x", "value": "one"})
            assert result["ok"] and result["committed"]
            await eventually(lambda: all(r.node.machine == {"x": "one"} for r in live.values()))

            follower_id = next(node_id for node_id in live if node_id != leader.node_id)
            follower = live.pop(follower_id)
            await follower.stop()
            persisted_term = follower.node.current_term
            leader, result = await commit_write(live, members, {"op": "set", "key": "y", "value": "two"})
            assert result["ok"]
            live[follower_id] = await shared_runtime(follower_id, members, tmp_path / f"{follower_id}.json", seed=100).start()
            assert live[follower_id].node.current_term >= persisted_term
            await eventually(lambda: all(r.node.machine == {"x": "one", "y": "two"} for r in live.values()))

            old_id = leader.node_id
            await live.pop(old_id).stop()
            replacement, result = await commit_write(live, members, {"op": "delete", "key": "x"})
            assert replacement.node_id != old_id
            assert result["ok"]
            live[old_id] = await shared_runtime(old_id, members, tmp_path / f"{old_id}.json", seed=200).start()
            await eventually(lambda: all(r.node.machine == {"y": "two"} for r in live.values()))
            status = await request(members[old_id], {"op": "status"})
            assert status["read_consistency"] == "local_stale"
            assert status["last_applied"] == status["commit_index"]
            assert all(r.failure is None for r in live.values())
        finally:
            await asyncio.gather(*(runtime.stop() for runtime in live.values()))
    asyncio.run(scenario())


def test_minority_write_returns_unknown_not_success(tmp_path: Path) -> None:
    async def scenario() -> None:
        members = addresses(3)
        live: dict[int, Runtime] = {}
        try:
            for node_id in members:
                live[node_id] = await shared_runtime(node_id, members, tmp_path / f"{node_id}.json",
                                                     seed=node_id, client_timeout=0.2).start()
            leader = await eventually(lambda: leader_in(live))
            followers = [node_id for node_id in live if node_id != leader.node_id]
            for node_id in followers:
                await live.pop(node_id).stop()
            result = await request(members[leader.node_id], {"op": "set", "key": "uncertain", "value": "v"})
            assert result["ok"] is False
            assert result["error"] == "timeout"
            assert result["outcome"] == "unknown"
            assert "uncertain" not in leader.node.machine
            for node_id in followers:
                live[node_id] = await shared_runtime(node_id, members, tmp_path / f"{node_id}.json", seed=node_id).start()
            await eventually(lambda: all(r.node.machine.get("uncertain") == "v" for r in live.values()))
        finally:
            await asyncio.gather(*(runtime.stop() for runtime in live.values()))
    asyncio.run(scenario())


def test_malformed_tcp_frames_do_not_mutate_raft_state(tmp_path: Path) -> None:
    async def scenario() -> None:
        members = addresses(3)
        live: dict[int, Runtime] = {}
        try:
            for node_id in members:
                live[node_id] = await shared_runtime(node_id, members, tmp_path / f"{node_id}.json", seed=node_id).start()
            leader = await eventually(lambda: leader_in(live))
            before = (leader.node.current_term, list(leader.node.log), dict(leader.node.machine))
            for frame in (
                b"not-json\n", b"[]\n",
                b'{"kind":"rpc","source":true,"message":{}}\n',
                b'{"kind":"rpc","source":2,"message":{"term":999999}}\n',
                b'{"kind":"client","op":"set","key":{},"value":"v"}\n',
            ):
                reader, writer = await asyncio.open_connection(*members[leader.node_id])
                try:
                    writer.write(frame)
                    await writer.drain()
                    response = json.loads(await asyncio.wait_for(reader.readline(), 1))
                    assert response["error"] == "invalid_request"
                finally:
                    writer.close()
                    await writer.wait_closed()
            assert (leader.node.current_term, list(leader.node.log), dict(leader.node.machine)) == before
            assert leader.failure is None
            follower_id = next(node_id for node_id in live if node_id != leader.node_id)
            response = await request(members[follower_id], {"op": "set", "key": "x", "value": "v"})
            assert response["error"] == "not_leader"
            assert response["leader_id"] == leader.node_id
        finally:
            await asyncio.gather(*(runtime.stop() for runtime in live.values()))
    asyncio.run(scenario())


def test_address_configuration_validation() -> None:
    assert parse_address("localhost:7101") == ("localhost", 7101)
    assert parse_address("[::1]:7101") == ("::1", 7101)
    for value in ("localhost", "localhost:0", ":123", "localhost:70000"):
        with pytest.raises(ValueError):
            parse_address(value)
    with pytest.raises(ValueError):
        parse_peers("1=127.0.0.1:7101,1=127.0.0.1:7102,3=127.0.0.1:7103")
    with pytest.raises(ValueError):
        parse_peers("1=127.0.0.1:7101,2=127.0.0.1:7102")


def test_reused_log_index_does_not_lose_new_client_waiter(tmp_path: Path) -> None:
    """Two overlapping requests can occupy the same index in different terms."""
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        sent = []
        runtime = Runtime(1, addresses(3), tmp_path / "unused.json", client_timeout=0.3)
        node = Node(1, [2, 3], MemoryStorage(), random.Random(1),
                    lambda peer, message: sent.append((peer, message)), now=loop.time())
        runtime.node = node
        node.start_election(loop.time())
        node.receive(2, VoteResponse(1, True), loop.time())
        node.propose(Command("set", "prefix", "uncommitted"), loop.time())
        old_request = asyncio.create_task(runtime._submit(Command("set", "old", "value")))
        await asyncio.sleep(0)
        assert node.last_log_index == 3
        # A legitimate later leader replaces this node's entirely uncommitted
        # suffix. This node then wins another term and reuses client index 3.
        node.receive(2, AppendEntries(2, 2, 0, 0, (LogEntry(2, Command("noop")),), 0), loop.time())
        node.start_election(loop.time())
        node.receive(2, VoteResponse(3, True), loop.time())
        runtime._resolve_clients()
        new_request = asyncio.create_task(runtime._submit(Command("set", "new", "value")))
        await asyncio.sleep(0)
        old_response = await old_request
        assert old_response["error"] == "leadership_changed"
        append = next(message for peer, message in reversed(sent)
                      if peer == 2 and isinstance(message, AppendEntries))
        node.receive(2, AppendResponse(3, True, 3, request_id=append.request_id), loop.time())
        runtime._resolve_clients()
        new_response = await new_request
        assert new_response["committed"] is True
        assert new_response["index"] == old_response["index"] == 3
        assert node.machine == {"new": "value"}
    asyncio.run(scenario())


def test_disk_failure_stops_runtime_instead_of_acknowledging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        members = addresses(3)
        live: dict[int, Runtime] = {}
        try:
            for node_id in members:
                live[node_id] = await shared_runtime(node_id, members, tmp_path / f"{node_id}.json", seed=node_id).start()
            leader = await eventually(lambda: leader_in(live))

            def failed_save(_state: Any) -> None:
                raise OSError("injected disk failure")

            monkeypatch.setattr(leader.node.storage, "save", failed_save)
            with pytest.raises((OSError, ValueError)):
                await request(members[leader.node_id], {"op": "set", "key": "unsafe", "value": "v"})
            with pytest.raises(StorageFailure):
                await asyncio.wait_for(leader.wait_failure(), 2)
            assert not leader.server.is_serving()
            assert "unsafe" not in leader.node.machine
            await leader.stop()
            assert not leader._tasks and not leader._handlers
        finally:
            await asyncio.gather(*(runtime.stop() for runtime in live.values()))
    asyncio.run(scenario())


@pytest.mark.parametrize("count", [3, 5])
def test_cli_independent_processes_survive_leader_kill(tmp_path: Path, count: int) -> None:
    root = Path(__file__).resolve().parents[1]
    members = addresses(count)
    peer_text = ",".join(f"{node_id}={host}:{port}" for node_id, (host, port) in members.items())
    processes: dict[int, subprocess.Popen[str]] = {}
    logs: dict[int, Any] = {}

    def launch(node_id: int) -> None:
        logs[node_id] = (tmp_path / f"process-{node_id}.log").open("a", encoding="utf-8")
        processes[node_id] = subprocess.Popen(
            [sys.executable, str(root / "raft.py"), "node", "--id", str(node_id),
             "--peers", peer_text, "--data", str(tmp_path / f"node-{node_id}.json"),
             "--seed", str(node_id)],
            cwd=root, stdout=logs[node_id], stderr=subprocess.STDOUT, text=True,
        )

    async def find_leader() -> int:
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            for node_id, process in processes.items():
                assert process.poll() is None, (tmp_path / f"process-{node_id}.log").read_text()
                try:
                    status = await request(members[node_id], {"op": "status"}, timeout=1.0)
                except (OSError, asyncio.TimeoutError):
                    continue
                if status["role"] == "leader":
                    return node_id
            await asyncio.sleep(0.02)
        raise AssertionError("independent node processes failed to elect a leader")

    def cli_set(value: str) -> int:
        deadline = time.monotonic() + 15
        last_output = ""
        while time.monotonic() < deadline:
            leader_id = asyncio.run(find_leader())
            host, port = members[leader_id]
            result = subprocess.run(
                [sys.executable, str(root / "raft.py"), "client", "--address", f"{host}:{port}",
                 "set", "process-test", value], cwd=root, capture_output=True, text=True, timeout=10,
            )
            last_output = result.stdout + result.stderr
            if result.returncode == 0:
                payload = json.loads(result.stdout)
                assert payload["committed"] is True
                return leader_id
            try:
                payload = json.loads(result.stdout)
            except json.JSONDecodeError:
                break
            if payload.get("error") not in {"not_leader", "leadership_changed", "timeout", "transport"}:
                break
            time.sleep(0.05)
        raise AssertionError(f"CLI client write did not commit within deadline: {last_output}")

    async def converge() -> None:
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            states = []
            for node_id in members:
                try:
                    states.append(await request(members[node_id], {"op": "status"}, timeout=1.0))
                except (OSError, asyncio.TimeoutError):
                    break
            if len(states) == count and all(s["state"].get("process-test") == "after" for s in states):
                return
            await asyncio.sleep(0.02)
        raise AssertionError("restarted process failed to recover committed state")

    try:
        for node_id in members:
            launch(node_id)
        original = cli_set("before")
        killed = processes.pop(original)
        killed.kill()
        killed.wait(timeout=5)
        logs.pop(original).close()
        replacement = cli_set("after")
        assert replacement != original
        launch(original)
        asyncio.run(converge())
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
