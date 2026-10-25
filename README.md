# RaftLight

Fixed-membership Raft for three or five Python nodes, with durable voting and logs, an asyncio TCP runtime, deterministic network faults and historical safety checks. The report is `report/raftlight.pdf` (also mirrored as `report/main.pdf`); its LaTeX source is `report/raftlight.tex` (`report/main.tex`).

One `Node` state machine runs in both the simulator and the TCP runtime. It implements randomized elections, log freshness checks, conflict repair, current-term majority commitment and ordered application to a string key-value map. The workload supports SET and DELETE. A leader appends a no-op after election so earlier entries can become committed without a new client write.

## Install

Use Python 3.12 or newer. The protocol runtime uses the standard library; tests and simulation plots use the packages in `requirements.txt`.

```bash
python -m venv .venv
```

Activate with `source .venv/bin/activate` on macOS/Linux or `.\.venv\Scripts\Activate.ps1` in PowerShell, then run:

```bash
python -m pip install -r requirements.txt
```

## Run a localhost cluster

Run one command in each of three terminals:

```bash
python raft.py node --id 1 --peers "1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103" --data data/node1.json
python raft.py node --id 2 --peers "1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103" --data data/node2.json
python raft.py node --id 3 --peers "1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103" --data data/node3.json
```

For five nodes, add `4=127.0.0.1:7104,5=127.0.0.1:7105` to the same membership string in all five commands. Each process needs its own ID and state file. Membership is fixed while running.

Find the leader using a status request:

```bash
python raft.py client --address 127.0.0.1:7101 status
```

Send writes to the reported leader address:

```bash
python raft.py client --address 127.0.0.1:7101 set x 10
python raft.py client --address 127.0.0.1:7101 delete x
```

A write succeeds only when its exact entry commits and applies. Timeout or leadership change leaves an unknown outcome: the command may commit later, and retries have no client-session deduplication guarantee. Status reads are local and may be stale. Stop with Ctrl+C; restarting with the same state file restores term, vote and log. Only one process may own a state file. The transport assumes trusted localhost peers.

## Reproduce the experiments

Run from the repository root:

```bash
python -m pytest -q
python test_network_hell.py --faults 10000 --seed 20261022 --trials 20
python benchmarks/run_benchmarks.py --trials 100 --writes 5 --election-trials 200 --recovery-trials 20
python tools/run_sensitivity.py
python benchmarks/run_tcp_benchmark.py --writes 100 --seed 42 --data-dir data/tcp-run
```

The TCP benchmark launches three independent node processes with FileStorage and measures 100 sequential writes through the public TCP request function. It selects localhost ports automatically and requires a fresh state directory. On repeated runs, pass `--data-dir PATH` with a new path, or use `make TCP_DATA=data/tcp-run-2 tcp`. Startup, initial election and final status checks are excluded from write latency. Connection setup, serialization, commitment, response and connection closure are included.

The saved run committed all 100 writes and verified every node's application state and durable command sequence. Median client-visible latency was 15.96 ms, with p95 of 30.45 ms on Windows 11, Python 3.12.14 and NTFS. The physical storage device was not identified. The result is a single host measurement, with background load uncontrolled.

The simulation campaign counts 500 effective actions in each of 20 runs, alternating three and five nodes. Faults, effective repairs and malformed-input checks count; ordinary ticks and periodic client writes do not. Final stable recovery is separate from the action budget. Original seeds and observations remain in the JSON files.

Simulation latency uses 100 independent seeds with five offered writes each, at nine packet-loss rates. Failure outcomes remain in the availability denominator and are excluded from successful-latency quantiles. The bootstrap resamples seed blocks, retaining dependence among sequential writes. The no-loss median is 8.03 simulated ms; at 80% loss, 128 of 500 attempts commit. Election experiments measure 800 phases across 400 cluster trials. Recovery experiments use 20 seeds for each of four scenarios.

To redraw the figures from saved observations:

```bash
python benchmarks/run_benchmarks.py --figures-only
```

To prepare the numeric tables:

```bash
python tools/build_report.py --tables-only
```

Open `report/raftlight.tex` (or `report/main.tex`) in a LaTeX editor, or run `latexmk -pdf raftlight.tex` from the `report/` directory. The optional `python tools/build_report.py` helper refreshes `report/tables.tex`, syncs `report/main.tex`, and compiles both `report/raftlight.pdf` and `report/main.pdf` when Tectonic or pdfLaTeX is installed; `--compiler PATH` selects an explicit compiler. GNU Make targets (`make test`, `make chaos`, `make benchmark`, `make sensitivity`, `make tcp`, `make report`, `make all`) wrap the five commands above; on Windows without GNU Make, run those Python commands directly in PowerShell.

## Files

| Path | Contents |
|---|---|
| `raftlight/node.py` | Transport-independent consensus state machine |
| `raftlight/messages.py` | Immutable RPCs and strict wire validation |
| `raftlight/storage.py` | Memory snapshots and atomic file persistence |
| `raftlight/transport.py` | asyncio listeners, peer queues and committed-write adapter |
| `raftlight/simulator.py` | Seeded delivery queue and historical safety checks |
| `tests/` | Protocol, storage, generated, oracle and real TCP tests |
| `results/chaos_summary.json`, `chaos-trace-*.json` | Campaign conditions, counts and individual traces |
| `results/benchmark_raw.json`, `benchmark_summary.json` | Simulated observations, endpoints, statistics and seeds |
| `results/tcp_benchmark.json` | All 100 real timings, responses, environment and final-state checks |
| `results/sensitivity_summary.json` | Five executed mutants, controls and first detected assertions |
| `results/pytest.xml` | Prior 74-test result for the unchanged protocol implementation |
| `results/requirements-lock.txt` | Package versions used for the experiments |
| `report/figures/` | Four empirical PDF/PNG plot pairs |
| `report/raftlight.tex`, `main.tex`, `tables.tex`, `raftlight.pdf`, `main.pdf` | Report sources, numeric tables and compiled reports |

Hypothesis uses 80 examples with up to 70 rule steps, both cluster sizes and shrinking on failure. A failure saves the action sequence and node/network diagnostics. The campaign saves failure snapshots and propagates invariant failures. Replaying a trace requires the recorded arguments and source.

## Scope and limits

Durable votes, current-term commitment, RPC response correlation and historical safety checks cover the core crash-fault protocol. The implementation omits dynamic membership, snapshots, compaction, authenticated peers, linearizable reads and client-session deduplication. Full JSON writes block the event loop and grow with log size; host power loss and disk tearing are not tested. Portable directory fsync is unavailable on Windows.

Simulation excludes disk, CPU and serialization costs and uses simple independent delays and loss. Finite tests, mutation witnesses and sampled seeds do not establish a formal proof. The five mutants are isolated witnesses with correct controls; their recorded assertions distinguish storage consistency failures from global consensus properties.

Algorithm reference: Ongaro and Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf), especially Figures 2 and 8. Generated testing uses [Hypothesis stateful tests](https://hypothesis.readthedocs.io/en/latest/stateful.html).

## Authorship and AI Disclosure

Engineered, tested, and verified by @gsd-ops, including system architecture, fault-injection scenarios, and protocol verification. AI coding assistants were used to accelerate code scaffolding, test-harness boilerplate, and LaTeX typesetting; all consensus state-machine logic, debugging, chaos simulations, and benchmarks were executed and validated locally.
