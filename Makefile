PYTHON ?= python
TCP_DATA ?= data/tcp-run
.PHONY: test chaos benchmark tcp sensitivity report all
test:
	$(PYTHON) -m pytest -q
chaos:
	$(PYTHON) test_network_hell.py --faults 10000 --seed 20261022 --trials 20
benchmark:
	$(PYTHON) benchmarks/run_benchmarks.py --trials 100 --writes 5 --election-trials 200 --recovery-trials 20
tcp:
	$(PYTHON) benchmarks/run_tcp_benchmark.py --writes 100 --seed 42 --data-dir "$(TCP_DATA)"
sensitivity:
	$(PYTHON) tools/run_sensitivity.py
report:
	$(PYTHON) tools/build_report.py
all:
	$(MAKE) test
	$(MAKE) chaos
	$(MAKE) benchmark
	$(MAKE) sensitivity
	$(MAKE) tcp
	$(MAKE) report
