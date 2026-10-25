"""Immutable Raft RPCs and defensive, explicit JSON boundary validation."""

from dataclasses import dataclass
from typing import Any, TypeAlias

from .state_machine import Command


def _integer(value: Any, name: str, minimum: int = 0) -> None:
    # bool is an int subclass, but accepting it would make the wire schema vague.
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")


def _fields(value: Any, expected: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{name} has missing or unknown fields")
    return value


@dataclass(frozen=True, slots=True)
class LogEntry:
    term: int
    command: Command

    def __post_init__(self) -> None:
        _integer(self.term, "entry term")
        if not isinstance(self.command, Command):
            raise ValueError("entry command must be a Command")


@dataclass(frozen=True, slots=True)
class RequestVote:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int

    def __post_init__(self) -> None:
        for name in ("term", "candidate_id", "last_log_index", "last_log_term"):
            _integer(getattr(self, name), name)
        if self.last_log_term > self.term:
            raise ValueError("last log term exceeds request term")
        if (self.last_log_index == 0) != (self.last_log_term == 0):
            raise ValueError("only the dummy entry has index/term zero")


@dataclass(frozen=True, slots=True)
class VoteResponse:
    term: int
    vote_granted: bool

    def __post_init__(self) -> None:
        _integer(self.term, "term")
        if type(self.vote_granted) is not bool:
            raise ValueError("vote_granted must be a boolean")


@dataclass(frozen=True, slots=True)
class AppendEntries:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int
    request_id: int = 0

    def __post_init__(self) -> None:
        for name in ("term", "leader_id", "prev_log_index", "prev_log_term",
                     "leader_commit", "request_id"):
            _integer(getattr(self, name), name)
        if (self.prev_log_index == 0) != (self.prev_log_term == 0):
            raise ValueError("only the dummy entry has index/term zero")
        if not isinstance(self.entries, tuple):
            raise ValueError("entries must be an immutable tuple")
        previous_term = self.prev_log_term
        for entry in self.entries:
            if not isinstance(entry, LogEntry) or entry.term == 0:
                raise ValueError("replicated entries must have positive terms")
            if entry.term < previous_term or entry.term > self.term:
                raise ValueError("entry terms must be ordered and <= leader term")
            previous_term = entry.term
        if self.prev_log_term > self.term:
            raise ValueError("previous log term exceeds leader term")


@dataclass(frozen=True, slots=True)
class AppendResponse:
    term: int
    success: bool
    match_index: int = 0
    conflict_index: int = 1
    conflict_term: int | None = None
    request_id: int = 0

    def __post_init__(self) -> None:
        for name in ("term", "match_index", "request_id"):
            _integer(getattr(self, name), name)
        _integer(self.conflict_index, "conflict_index", 1)
        if type(self.success) is not bool:
            raise ValueError("success must be a boolean")
        if self.conflict_term is not None:
            _integer(self.conflict_term, "conflict_term")
            if self.conflict_term > self.term:
                raise ValueError("conflict term exceeds response term")


Message: TypeAlias = RequestVote | VoteResponse | AppendEntries | AppendResponse


def command_to_dict(command: Command) -> dict[str, Any]:
    return {"op": command.op, "key": command.key, "value": command.value}


def command_from_dict(data: Any) -> Command:
    value = _fields(data, {"op", "key", "value"}, "command")
    return Command(**value)


def entry_to_dict(entry: LogEntry) -> dict[str, Any]:
    return {"term": entry.term, "command": command_to_dict(entry.command)}


def entry_from_dict(data: Any) -> LogEntry:
    value = _fields(data, {"term", "command"}, "log entry")
    return LogEntry(value["term"], command_from_dict(value["command"]))


def message_to_dict(message: Message) -> dict[str, Any]:
    """Encode the wire schema without relying on Python class deserialization."""
    if isinstance(message, RequestVote):
        return {"type": "RequestVote", "term": message.term,
                "candidate_id": message.candidate_id,
                "last_log_index": message.last_log_index,
                "last_log_term": message.last_log_term}
    if isinstance(message, VoteResponse):
        return {"type": "VoteResponse", "term": message.term,
                "vote_granted": message.vote_granted}
    if isinstance(message, AppendEntries):
        return {"type": "AppendEntries", "term": message.term,
                "leader_id": message.leader_id,
                "prev_log_index": message.prev_log_index,
                "prev_log_term": message.prev_log_term,
                "entries": [entry_to_dict(entry) for entry in message.entries],
                "leader_commit": message.leader_commit,
                "request_id": message.request_id}
    if isinstance(message, AppendResponse):
        return {"type": "AppendResponse", "term": message.term,
                "success": message.success, "match_index": message.match_index,
                "conflict_index": message.conflict_index,
                "conflict_term": message.conflict_term,
                "request_id": message.request_id}
    raise ValueError("unknown message type")


def message_from_dict(data: Any) -> Message:
    """Reject missing fields, surplus fields, invalid types, and invalid values."""
    if not isinstance(data, dict) or not isinstance(data.get("type"), str):
        raise ValueError("message must be an object with a string type")
    classes = {"RequestVote": RequestVote, "VoteResponse": VoteResponse,
               "AppendEntries": AppendEntries, "AppendResponse": AppendResponse}
    cls = classes.get(data["type"])
    if cls is None:
        raise ValueError("unknown message type")
    _fields(data, {"type", *cls.__dataclass_fields__}, "message")
    values = {key: value for key, value in data.items() if key != "type"}
    if cls is AppendEntries:
        if not isinstance(values["entries"], list):
            raise ValueError("wire entries must be an array")
        values["entries"] = tuple(entry_from_dict(entry) for entry in values["entries"])
    return cls(**values)
