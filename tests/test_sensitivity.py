"""Paired actual-protocol sensitivity experiments, separate from state corruption controls."""

import pytest

from raftlight.sensitivity import run_sensitivity


@pytest.fixture(scope="module")
def sensitivity():
    return run_sensitivity()


@pytest.mark.parametrize("name,expected", [
    ("NoElectionRestriction", "Leader Completeness"),
    ("DoubleVote", "Persisted-vote consistency"),
    ("ForgetVoteOnRestart", "Durable/volatile state agreement"),
    ("NaiveCommit", "Leader Completeness"),
    ("CommitBeyondMatch", "Committed-entry agreement"),
])
def test_protocol_mutant_has_passing_control_and_real_monitor_detection(sensitivity, name, expected):
    row = next(row for row in sensitivity["experiments"] if row["mutant"] == name)
    assert row["correct_control_pass"], row["control_failure"]
    assert row["detected"]
    assert row["invariant"] == expected
    assert row["assertion"].startswith("assert ")
    assert row["location"].startswith("raftlight/simulator.py:")
    assert any("first_monitor_detection" in step for step in row["trace"])
    assert not any("first_monitor_detection" in step for step in row["control_trace"])


def test_forget_vote_witness_actually_restarts_and_preserves_term_and_log(sensitivity):
    row = next(row for row in sensitivity["experiments"] if row["mutant"] == "ForgetVoteOnRestart")
    for arm in ("control", "mutant"):
        witness = row["restart_witness"][arm]
        assert witness["crash_restart_executed"]
        assert witness["term_preserved"] and witness["log_preserved"]
        assert witness["first_vote_granted"]
    assert row["restart_witness"]["mutant"]["second_vote_granted"]
    assert not row["restart_witness"]["control"]["second_vote_granted"]


def test_beyond_match_uses_real_eight_entry_batch(sensitivity):
    row = next(row for row in sensitivity["experiments"] if row["mutant"] == "CommitBeyondMatch")
    request = row["trace"][-1]["message"]
    assert request["type"] == "AppendEntries"
    assert len(request["entries"]) == 8
    assert request["prev_log_index"] == 0 and request["leader_commit"] == 11


def test_figure8_counts_old_quorum_before_later_conflicting_leader(sensitivity):
    row = next(row for row in sensitivity["experiments"] if row["mutant"] == "NaiveCommit")
    bad = next(step for step in row["trace"] if step["action"] == "withhold_current_term_noop")
    good = next(step for step in row["control_trace"] if step["action"] == "withhold_current_term_noop")
    assert bad["observed_commit_index"] == 8
    assert good["observed_commit_index"] == 0
    assert row["first_detection_snapshot"]["nodes"][5]["term"] == 5


def test_sensitivity_replays_deterministically(sensitivity):
    assert run_sensitivity() == sensitivity
    assert sensitivity["mutants_total"] == sensitivity["detected_total"] == sensitivity["correct_controls_passed"] == 5
