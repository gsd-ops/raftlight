"""Deterministic application commands shared by simulation and the TCP runtime."""

from dataclasses import dataclass
from typing import Literal, MutableMapping


@dataclass(frozen=True, slots=True)
class Command:
    """A string key/value operation; no-op entries establish a leader's term.

    Reads of the local mapping are observational and are not linearizable.
    Retrying a write may add another log entry: client deduplication is outside
    this deliberately small implementation.
    """

    op: Literal["set", "delete", "noop"]
    key: str = ""
    value: str = ""

    def __post_init__(self) -> None:
        if self.op not in ("set", "delete", "noop"):
            raise ValueError("unknown command operation")
        if not isinstance(self.key, str) or not isinstance(self.value, str):
            raise ValueError("command keys and values must be strings")
        if self.op != "noop" and not self.key:
            raise ValueError("set/delete require a nonempty key")
        if self.op == "noop" and (self.key or self.value):
            raise ValueError("noop must not contain a key or value")
        if self.op == "delete" and self.value:
            raise ValueError("delete must not contain a value")


def apply_command(state: MutableMapping[str, str], command: Command) -> None:
    """Apply exactly one already-committed operation; delete is idempotent."""
    if command.op == "set":
        state[command.key] = command.value
    elif command.op == "delete":
        state.pop(command.key, None)
    elif command.op != "noop":
        raise ValueError("unknown command operation")
