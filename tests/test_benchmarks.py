"""Check measured statistics and failure denominators without changing results."""
from types import SimpleNamespace

import pytest

from benchmarks.run_benchmarks import election_metrics, latency_intervals, quantiles


def test_percentiles_are_unmodified_sample_estimates():
    values = [0.001, 0.002, 0.010, 0.100]
    result = quantiles(values)
    assert result["samples"] == 4
    assert result["p50_ms"] == pytest.approx(6)
    assert result["p95_ms"] == pytest.approx(86.5)
    assert result["p99_ms"] == pytest.approx(97.3)
    assert quantiles([]) == {"p50_ms": None, "p95_ms": None, "p99_ms": None, "samples": 0}


def test_seed_bootstrap_keeps_failure_denominator_and_within_seed_pairs():
    # Each seed always offers exactly one successful and one failed operation.
    # Resampling individual observations would wrongly vary this 50% rate.
    rows = [dict(seed=seed, outcome=outcome, latency_s=.01 if outcome == "committed" else None)
            for seed in range(4) for outcome in ("committed", "timeout")]
    result = latency_intervals(rows, seed=123, repetitions=100)
    assert result["success_95ci_percent"] == [50, 50]
    assert result["bootstrap_95ci_ms"] == {name: [10, 10] for name in ("p50", "p95", "p99")}
    assert result["independent_seeds"] == 4
    assert result == latency_intervals(rows, seed=123, repetitions=100)


def test_no_successful_latency_has_no_fabricated_interval():
    rows = [dict(seed=seed, outcome="no_leader", latency_s=None) for seed in range(3)]
    result = latency_intervals(rows, seed=1, repetitions=20)
    assert result["success_95ci_percent"] == [0, 0]
    assert result["bootstrap_nonempty_latency_resamples"] == 0
    assert result["bootstrap_95ci_ms"] == {name: [None, None] for name in ("p50", "p95", "p99")}


def test_split_vote_counts_failed_contested_terms_in_measurement_phase():
    monitor = SimpleNamespace(
        election_history=[dict(time=.1, term=1, node=1), dict(time=.1, term=1, node=2),
                          dict(time=.2, term=2, node=1), dict(time=.2, term=2, node=2),
                          dict(time=.3, term=3, node=3)],
        leaders_by_term={1: 1, 3: 3},
    )
    metrics = election_metrics(SimpleNamespace(monitor=monitor), since=.2)
    assert metrics["election_attempts"] == 3
    assert metrics["contested_terms"] == 1
    assert metrics["split_vote_terms"] == 1
    assert metrics["split_vote_observed"] is True
