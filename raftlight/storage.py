"""Durable term/vote/log state. Commit/application positions are volatile.

FileStorage writes a complete JSON snapshot via fsync + atomic replace. It is
intended for one node process per file, on a local filesystem with reliable
replace/fsync semantics. It is not a concurrent database or a write-ahead log.
"""

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import tempfile
from typing import Protocol

from .messages import LogEntry, _integer, entry_from_dict, entry_to_dict
from .state_machine import Command


def _empty_log() -> tuple[LogEntry, ...]:
    return (LogEntry(0, Command("noop")),)


@dataclass(frozen=True, slots=True)
class PersistentState:
    current_term: int = 0
    voted_for: int | None = None
    log: tuple[LogEntry, ...] = field(default_factory=_empty_log)

    def __post_init__(self) -> None:
        _integer(self.current_term, "current_term")
        if self.voted_for is not None:
            _integer(self.voted_for, "voted_for")
        if not isinstance(self.log, tuple) or not self.log or self.log[0] != _empty_log()[0]:
            raise ValueError("persistent log must be a tuple beginning with the dummy entry")
        previous_term = 0
        for entry in self.log[1:]:
            if not isinstance(entry, LogEntry) or entry.term == 0:
                raise ValueError("persistent log contains an invalid entry")
            if entry.term < previous_term or entry.term > self.current_term:
                raise ValueError("persistent log terms are inconsistent")
            previous_term = entry.term


class Storage(Protocol):
    def load(self) -> PersistentState:
        """Return an immutable durable snapshot."""
        ...

    def save(self, state: PersistentState) -> None:
        """Make the entire new snapshot durable before returning."""
        ...


def _check_transition(before: PersistentState, after: PersistentState) -> None:
    if not isinstance(after, PersistentState):
        raise ValueError("storage accepts PersistentState snapshots")
    if after.current_term < before.current_term:
        raise ValueError("persisted terms cannot decrease")
    if (after.current_term == before.current_term and before.voted_for is not None
            and after.voted_for != before.voted_for):
        raise ValueError("a persisted vote cannot change within a term")


class MemoryStorage:
    """An immutable snapshot survives destruction/recreation of its Node."""

    def __init__(self, initial: PersistentState | None = None) -> None:
        self._state = initial if initial is not None else PersistentState()
        if not isinstance(self._state, PersistentState):
            raise ValueError("initial state must be PersistentState")

    def load(self) -> PersistentState:
        return self._state

    def save(self, state: PersistentState) -> None:
        _check_transition(self._state, state)
        self._state = state


class StorageCorruption(ValueError):
    """A durable snapshot is invalid; never silently reset a damaged node."""


class StorageFailure(RuntimeError):
    """Consensus must stop after persistence fails; a network retry is unsafe."""


class FileStorage:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._read()

    def _read(self) -> PersistentState:
        if not self.path.exists():
            return PersistentState()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or set(data) != {"version", "current_term", "voted_for", "log"}:
                raise ValueError("invalid durable snapshot schema")
            if type(data["version"]) is not int or data["version"] != 1:
                raise ValueError("unsupported durable snapshot version")
            if not isinstance(data["log"], list):
                raise ValueError("durable log must be an array")
            return PersistentState(data["current_term"], data["voted_for"],
                                   tuple(entry_from_dict(entry) for entry in data["log"]))
        except (ValueError, TypeError, KeyError) as error:
            raise StorageCorruption(f"invalid Raft state in {self.path}: {error}") from error

    def load(self) -> PersistentState:
        return self._state

    def save(self, state: PersistentState) -> None:
        _check_transition(self._state, state)
        data = {"version": 1, "current_term": state.current_term,
                "voted_for": state.voted_for,
                "log": [entry_to_dict(entry) for entry in state.log]}
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                             dir=self.path.parent,
                                             prefix=self.path.name + ".", suffix=".tmp",
                                             delete=False) as handle:
                temporary = handle.name
                json.dump(data, handle, separators=(",", ":"), ensure_ascii=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = None
            # POSIX requires directory fsync to persist a changed directory entry.
            # Windows has no portable Python directory-fsync API.
            if os.name == "posix":
                descriptor = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            self._state = state
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)
