#!/usr/bin/env python3
"""Run a localhost RaftLight node or submit a committed-write client request."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
import signal

from raftlight.transport import Runtime, parse_address, parse_peers, request


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    modes = result.add_subparsers(dest="mode", required=True)
    node = modes.add_parser("node", help="run one independent, durable Raft node")
    node.add_argument("--id", type=int, required=True)
    node.add_argument("--peers", type=parse_peers, required=True,
                      help="all 3 or 5 members, including self: 1=127.0.0.1:7101,...")
    node.add_argument("--data", type=Path, required=True, help="durable JSON state file unique to this node")
    node.add_argument("--seed", type=int, default=None, help="optional reproducible election random seed")
    node.add_argument("--client-timeout", type=float, default=3.0)
    node.add_argument("--verbose", action="store_true")
    client = modes.add_parser("client", help="submit to a known node; status reads may be stale")
    client.add_argument("--address", type=parse_address, required=True)
    client.add_argument("--timeout", type=float, default=5.0)
    commands = client.add_subparsers(dest="operation", required=True)
    commands.add_parser("status", help="local potentially stale state and leader hint")
    set_command = commands.add_parser("set")
    set_command.add_argument("key")
    set_command.add_argument("value")
    delete_command = commands.add_parser("delete")
    delete_command.add_argument("key")
    return result


async def serve(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    loop = asyncio.get_running_loop()
    stopped = asyncio.Event()
    old_signals: dict[signal.Signals, signal.Handlers] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        old_signals[signum] = signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stopped.set))
    runtime = Runtime(args.id, args.peers, args.data, seed=args.seed, client_timeout=args.client_timeout)
    waiters: list[asyncio.Task[None | bool]] = []
    try:
        await runtime.start()
        logging.info("RaftLight node %s listening on %s:%s", args.id, *args.peers[args.id])
        waiters = [asyncio.create_task(stopped.wait()), asyncio.create_task(runtime.wait_failure())]
        done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in waiters:
            task.cancel()
        if waiters:
            await asyncio.gather(*waiters, return_exceptions=True)
        await runtime.stop()
        for signum, handler in old_signals.items():
            signal.signal(signum, handler)


async def client(args: argparse.Namespace) -> int:
    payload = {"op": args.operation}
    if args.operation in {"set", "delete"}:
        payload["key"] = args.key
    if args.operation == "set":
        payload["value"] = args.value
    try:
        response = await request(args.address, payload, timeout=args.timeout)
    except (OSError, asyncio.TimeoutError, ValueError) as exc:
        response = {"ok": False, "error": "transport", "detail": str(exc), "outcome": "unknown"}
    print(json.dumps(response, sort_keys=True))
    return 0 if response.get("ok") is True else 1


def main() -> int:
    args = parser().parse_args()
    try:
        if args.mode == "node":
            asyncio.run(serve(args))
            return 0
        return asyncio.run(client(args))
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError) as exc:
        logging.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
