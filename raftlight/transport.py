"""Local TCP runtime for the same consensus engine used by the simulator.

Each connection carries one bounded newline-delimited JSON envelope. RPC replies
are ordinary Raft messages on independent connections; transport acknowledgements
never count as replication acknowledgements. This deliberately simple transport
is intended for trusted localhost peers, not Internet-facing deployment.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import random
from typing import Any

from .messages import (
    AppendEntries, LogEntry, Message, RequestVote,
    message_from_dict, message_to_dict,
)
from .node import Node, NotLeader, Role
from .state_machine import Command
from .storage import FileStorage

LOGGER = logging.getLogger(__name__)
MAX_FRAME = 1_048_576
Address = tuple[str, int]


def parse_address(value: str) -> Address:
    """Parse a host:port address (IPv4 or hostname; bracketed IPv6 supported)."""
    try:
        host, port_text = value.rsplit(":", 1)
        port = int(port_text)
    except (ValueError, AttributeError) as exc:
        raise ValueError("address must be HOST:PORT") from exc
    host = host.removeprefix("[").removesuffix("]")
    if not host or not 1 <= port <= 65535:
        raise ValueError("address needs a host and port in 1..65535")
    return host, port


def parse_peers(value: str) -> dict[int, Address]:
    """Parse all cluster members, including self, as ID=HOST:PORT pairs."""
    members: dict[int, Address] = {}
    try:
        for item in value.split(","):
            node_text, address_text = item.split("=", 1)
            node_id = int(node_text)
            if node_id < 0 or node_id in members:
                raise ValueError("node IDs must be unique nonnegative integers")
            members[node_id] = parse_address(address_text)
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"invalid peers: {exc}") from exc
    if len(members) not in (3, 5):
        raise ValueError("RaftLight runtime requires exactly 3 or 5 members")
    if len(set(members.values())) != len(members):
        raise ValueError("each member must have a distinct address")
    return members


def _encode(payload: dict[str, Any]) -> bytes:
    data = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"
    if len(data) > MAX_FRAME:
        raise ValueError("message exceeds the maximum frame size")
    return data


async def _read(reader: asyncio.StreamReader, timeout: float) -> dict[str, Any]:
    data = await asyncio.wait_for(reader.readline(), timeout)
    if not data or not data.endswith(b"\n") or len(data) > MAX_FRAME:
        raise ValueError("expected one bounded newline-delimited JSON object")
    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as exc:
        raise ValueError("invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("expected a JSON object")
    return payload


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 0.5)
    except (ConnectionError, OSError, asyncio.TimeoutError):
        # A disconnected peer is expected in a crash-fault runtime.
        pass


@dataclass
class _Pending:
    entry: LogEntry
    future: asyncio.Future[dict[str, Any]]


class Runtime:
    """An independent Raft node, durable file, TCP listener, and timer loop.

    All consensus transitions execute synchronously on one asyncio event loop.
    Slow or unavailable peers cannot block election timers: each has its own
    bounded outgoing queue. Queue overflow is equivalent to dropped RPCs.
    """

    def __init__(
        self,
        node_id: int,
        addresses: dict[int, Address],
        data_path: str | Path,
        *,
        seed: int | None = None,
        tick_interval: float = 0.01,
        rpc_timeout: float = 0.25,
        client_timeout: float = 3.0,
    ) -> None:
        if len(addresses) not in (3, 5) or node_id not in addresses:
            raise ValueError("runtime needs a 3- or 5-member cluster containing self")
        if min(tick_interval, rpc_timeout, client_timeout) <= 0:
            raise ValueError("timeouts and tick interval must be positive")
        self.node_id = node_id
        self.addresses = dict(addresses)
        self.data_path = Path(data_path)
        self.seed = seed
        self.tick_interval = tick_interval
        self.rpc_timeout = rpc_timeout
        self.client_timeout = client_timeout
        self.node: Node | None = None
        self.server: asyncio.Server | None = None
        self._queues: dict[int, asyncio.Queue[bytes]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._handlers: set[asyncio.Task[Any]] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._pending: dict[int, _Pending] = {}
        self._closed = True
        self._failed = asyncio.Event()
        self.failure: BaseException | None = None
        self.dropped_outbound = 0

    async def start(self) -> Runtime:
        if not self._closed:
            raise RuntimeError("runtime is already started")
        loop = asyncio.get_running_loop()
        self._closed = False
        self._failed.clear()
        self.failure = None
        self._queues = {
            peer: asyncio.Queue(maxsize=256)
            for peer in self.addresses if peer != self.node_id
        }
        try:
            self.node = Node(
                self.node_id, sorted(self._queues), FileStorage(self.data_path),
                random.Random(self.seed), self._send, now=loop.time(),
            )
            host, port = self.addresses[self.node_id]
            self.server = await asyncio.start_server(self._accept, host, port, limit=MAX_FRAME)
        except BaseException:
            self._closed = True
            raise
        for peer in self._queues:
            self._spawn(self._sender(peer), f"raft-{self.node_id}-to-{peer}")
        self._spawn(self._ticker(), f"raft-{self.node_id}-clock")
        return self

    def _spawn(self, coroutine: Any, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._background_done)

    def _background_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None and self.failure is None:
            self.failure = error
            self._failed.set()
            LOGGER.error("node %s background task failed", self.node_id, exc_info=error)
            # Storage/protocol bugs must stop this node before it can send another
            # vote or replication acknowledgement based on uncertain state.
            if self.server is not None:
                self.server.close()
            for active in self._tasks | self._handlers:
                active.cancel()
            for writer in self._writers:
                writer.close()

    async def wait_failure(self) -> None:
        """Wait for, then propagate, an unexpected background/runtime failure."""
        await self._failed.wait()
        assert self.failure is not None
        raise self.failure

    async def stop(self) -> None:
        """Close the listener, cancel timers/connections, and resolve waiters."""
        if self._closed:
            return
        self._closed = True
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.set_result({"ok": False, "error": "shutdown", "outcome": "unknown"})
        self._pending.clear()
        tasks = list(self._tasks | self._handlers)
        for task in tasks:
            task.cancel()
        for writer in list(self._writers):
            writer.close()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._handlers.clear()
        self._writers.clear()
        self._queues.clear()

    async def __aenter__(self) -> Runtime:
        return await self.start()

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    def _send(self, peer: int, message: Message) -> None:
        if self._closed or self.failure is not None:
            return
        frame = _encode({"kind": "rpc", "source": self.node_id, "message": message_to_dict(message)})
        try:
            self._queues[peer].put_nowait(frame)
        except asyncio.QueueFull:
            self.dropped_outbound += 1
            LOGGER.debug("node %s dropped queued RPC to %s", self.node_id, peer)

    async def _sender(self, peer: int) -> None:
        queue = self._queues[peer]
        while True:
            frame = await queue.get()
            writer: asyncio.StreamWriter | None = None
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(*self.addresses[peer], limit=MAX_FRAME),
                    self.rpc_timeout,
                )
                self._writers.add(writer)
                writer.write(frame)
                await asyncio.wait_for(writer.drain(), self.rpc_timeout)
                acknowledgement = await _read(reader, self.rpc_timeout)
                if acknowledgement.get("ok") is not True:
                    LOGGER.warning("peer %s rejected RPC: %s", peer, acknowledgement)
            except (OSError, ConnectionError, asyncio.TimeoutError, ValueError) as exc:
                LOGGER.debug("RPC %s -> %s unavailable: %s", self.node_id, peer, exc)
            finally:
                queue.task_done()
                if writer is not None:
                    self._writers.discard(writer)
                    await _close_writer(writer)

    async def _ticker(self) -> None:
        assert self.node is not None
        loop = asyncio.get_running_loop()
        while True:
            self.node.tick(loop.time())
            self._resolve_clients()
            await asyncio.sleep(self.tick_interval)

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._closed or self.failure is not None:
            writer.close()
            return
        task = asyncio.create_task(self._handle(reader, writer))
        self._handlers.add(task)
        task.add_done_callback(self._handler_done)

    def _handler_done(self, task: asyncio.Task[Any]) -> None:
        self._handlers.discard(task)
        self._background_done(task)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        try:
            try:
                envelope = await _read(reader, self.rpc_timeout)
                kind = envelope.get("kind")
                if kind == "rpc":
                    source, message = self._decode_rpc(envelope)
                    command = None
                elif kind == "client":
                    command = self._decode_client(envelope)
                else:
                    raise ValueError("unknown envelope kind")
            except (ValueError, asyncio.TimeoutError) as exc:
                response: dict[str, Any] = {"ok": False, "error": "invalid_request", "detail": str(exc)}
            else:
                assert self.node is not None
                if kind == "rpc":
                    self.node.receive(source, message, asyncio.get_running_loop().time())
                    self._resolve_clients()
                    response = {"ok": True}
                elif command is None:
                    response = self.status()
                else:
                    response = await self._submit(command)
            try:
                frame = _encode(response)
            except ValueError:
                frame = _encode({"ok": False, "error": "response_too_large",
                                 "detail": "local state exceeds the TCP status frame limit"})
            writer.write(frame)
            await asyncio.wait_for(writer.drain(), self.rpc_timeout)
        except (ConnectionError, OSError, asyncio.TimeoutError):
            LOGGER.debug("node %s client/peer disconnected", self.node_id)
        finally:
            self._writers.discard(writer)
            await _close_writer(writer)

    def _decode_rpc(self, envelope: dict[str, Any]) -> tuple[int, Message]:
        if set(envelope) != {"kind", "source", "message"}:
            raise ValueError("invalid RPC envelope fields")
        source = envelope["source"]
        if type(source) is not int or source not in self._queues:
            raise ValueError("RPC source must be a configured peer")
        message = message_from_dict(envelope["message"])
        if isinstance(message, RequestVote) and message.candidate_id != source:
            raise ValueError("candidate does not match RPC source")
        if isinstance(message, AppendEntries) and message.leader_id != source:
            raise ValueError("leader does not match RPC source")
        return source, message

    @staticmethod
    def _decode_client(envelope: dict[str, Any]) -> Command | None:
        operation = envelope.get("op")
        if operation == "status" and set(envelope) == {"kind", "op"}:
            return None
        if operation == "set" and set(envelope) == {"kind", "op", "key", "value"}:
            key, value = envelope["key"], envelope["value"]
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("keys and values must be strings")
            if len(key.encode("utf-8")) + len(value.encode("utf-8")) > 16384:
                raise ValueError("key and value together must not exceed 16 KiB")
            return Command("set", key, value)
        if operation == "delete" and set(envelope) == {"kind", "op", "key"}:
            key = envelope["key"]
            if not isinstance(key, str) or len(key.encode("utf-8")) > 16384:
                raise ValueError("key must be a string of at most 16 KiB")
            return Command("delete", key)
        raise ValueError("use status, set with key/value, or delete with key")

    def status(self) -> dict[str, Any]:
        """Return a local, potentially stale snapshot; no linearizable reads."""
        assert self.node is not None
        leader = self.node.leader_id
        address = self.addresses.get(leader) if leader is not None else None
        return {
            "ok": True, "node_id": self.node_id, "role": self.node.role.value,
            "term": self.node.current_term, "leader_id": leader,
            "leader_address": f"{address[0]}:{address[1]}" if address else None,
            "commit_index": self.node.commit_index, "last_applied": self.node.last_applied,
            "last_log_index": self.node.last_log_index,
            "state": dict(self.node.machine), "read_consistency": "local_stale",
        }

    async def _submit(self, command: Command) -> dict[str, Any]:
        assert self.node is not None
        loop = asyncio.get_running_loop()
        try:
            index = self.node.propose(command, loop.time())
        except NotLeader:
            status = self.status()
            return {"ok": False, "error": "not_leader", "leader_id": status["leader_id"],
                    "leader_address": status["leader_address"]}
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        pending = _Pending(self.node.log[index], future)
        self._pending[index] = pending
        self._resolve_clients()
        try:
            return await asyncio.wait_for(asyncio.shield(future), self.client_timeout)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "timeout", "outcome": "unknown", "index": index}
        finally:
            if self._pending.get(index) is pending:
                self._pending.pop(index)
            if not future.done():
                future.cancel()

    def _resolve_clients(self) -> None:
        assert self.node is not None
        for index, pending in self._pending.items():
            if pending.future.done():
                continue
            same_entry = index < len(self.node.log) and self.node.log[index] == pending.entry
            if same_entry and self.node.commit_index >= index:
                pending.future.set_result({"ok": True, "committed": True, "index": index,
                                           "term": pending.entry.term})
            elif (not same_entry or self.node.role != Role.LEADER
                  or self.node.current_term != pending.entry.term):
                pending.future.set_result({"ok": False, "error": "leadership_changed",
                                           "outcome": "unknown", "index": index})


async def request(address: Address, payload: dict[str, Any], *, timeout: float = 5.0) -> dict[str, Any]:
    """Send one client request. Connection errors propagate to the caller."""
    reader, writer = await asyncio.wait_for(asyncio.open_connection(*address, limit=MAX_FRAME), timeout)
    try:
        writer.write(_encode({"kind": "client", **payload}))
        await asyncio.wait_for(writer.drain(), timeout)
        return await _read(reader, timeout)
    finally:
        await _close_writer(writer)
