"""Durability ordering, atomic replacement, and fatal disk-failure regression tests."""

import json
import random

import pytest

from raftlight.messages import AppendEntries, LogEntry, RequestVote
from raftlight.node import Node
from raftlight.state_machine import Command
from raftlight.storage import (FileStorage, MemoryStorage, PersistentState,
                               StorageCorruption, StorageFailure)


def snapshot(term=2, voted_for=1, value="saved"):
    return PersistentState(term, voted_for,
                           (LogEntry(0, Command("noop")),
                            LogEntry(1, Command("set", "x", value))))


def test_file_snapshot_survives_new_storage_and_node(tmp_path):
    path = tmp_path / "node.json"
    store = FileStorage(path)
    state = snapshot()
    store.save(state)
    reopened = FileStorage(path)
    assert reopened.load() == state
    node = Node(1, [2, 3], reopened, random.Random(1), lambda *_: None)
    assert node.current_term == 2 and node.voted_for == 1
    assert tuple(node.log) == state.log
    # Recovery cannot guess which persisted entries were committed.
    assert node.commit_index == node.last_applied == 0 and node.machine == {}
    assert json.loads(path.read_text())["version"] == 1


@pytest.mark.parametrize("contents", ["not json", '{}',
    '{"version":1,"current_term":0,"voted_for":null,"log":[]}'])
def test_corrupt_snapshot_is_rejected_without_overwriting(tmp_path, contents):
    path = tmp_path / "node.json"
    path.write_text(contents)
    with pytest.raises(StorageCorruption):
        FileStorage(path)
    assert path.read_text() == contents


def test_failed_atomic_replace_keeps_previous_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "node.json"
    store = FileStorage(path)
    before = snapshot()
    store.save(before)

    def fail_replace(*_):
        raise OSError("injected replace failure")

    monkeypatch.setattr("raftlight.storage.os.replace", fail_replace)
    with pytest.raises(OSError, match="injected"):
        store.save(snapshot(3, 2, "new"))
    assert store.load() == FileStorage(path).load() == before
    assert list(tmp_path.glob("*.tmp")) == []


def test_storage_rejects_term_regression_and_same_term_double_vote():
    store = MemoryStorage(snapshot())
    with pytest.raises(ValueError, match="decrease"):
        store.save(snapshot(1, 1))
    with pytest.raises(ValueError, match="vote"):
        store.save(snapshot(2, 3))
    assert store.load() == snapshot()


def test_vote_and_append_are_durable_before_response():
    store = MemoryStorage()
    sent = []

    def send(peer, message):
        saved = store.load()
        assert saved.current_term == node.current_term
        assert saved.voted_for == node.voted_for
        assert saved.log == tuple(node.log)
        sent.append(message)

    node = Node(1, [2, 3], store, random.Random(1), send)
    node.receive(2, RequestVote(1, 2, 0, 0), 0)
    assert sent[-1].vote_granted and store.load().voted_for == 2
    entry = LogEntry(1, Command("set", "x", "durable"))
    node.receive(2, AppendEntries(1, 2, 0, 0, (entry,), 1, 1), .01)
    assert sent[-1].success and store.load().log[1] == entry
    assert node.machine == {"x": "durable"}


def test_persistence_failure_sends_no_ack_and_permanently_stops_node():
    class BrokenStorage(MemoryStorage):
        def save(self, state):
            raise OSError("injected disk failure")

    sent = []
    store = BrokenStorage()
    node = Node(1, [2, 3], store, random.Random(1), lambda *args: sent.append(args))
    with pytest.raises(StorageFailure):
        node.receive(2, RequestVote(1, 2, 0, 0), 0)
    assert sent == [] and store.load().current_term == 0
    with pytest.raises(StorageFailure):
        node.tick(1)
    with pytest.raises(StorageFailure):
        node.receive(3, RequestVote(2, 3, 0, 0), 1)
    assert sent == []


def test_same_term_conflicting_command_rejects_without_mutation():
    first = LogEntry(1, Command("set", "x", "original"))
    second = LogEntry(1, Command("set", "y", "original"))
    state = PersistentState(1, 2, (LogEntry(0, Command("noop")), first, second))
    store = MemoryStorage(state)
    sent = []
    node = Node(1, [2, 3], store, random.Random(1), lambda *args: sent.append(args))
    invalid = LogEntry(1, Command("set", "y", "different"))
    node.receive(2, AppendEntries(1, 2, 0, 0, (first, invalid), 2, 8), .01)
    assert not sent[-1][-1].success
    assert tuple(node.log) == state.log == store.load().log
    assert node.commit_index == 0 and node.machine == {}
