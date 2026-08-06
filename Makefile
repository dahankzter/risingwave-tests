# Test bench for risingwave-mr images. Podman-only (this machine has no docker); a single
# container needs no compose.

RW_IMAGE ?= ghcr.io/dahankzter/risingwave:v3.1.0-alpha--mr--bee0fbd--feat-match-recognize-v2
NAME     ?= rw-tests
# Whatever is on PATH. A Mac with keg-only Homebrew libpq does not put psql on PATH, so set it
# in the environment there:  export PSQL=/opt/homebrew/opt/libpq/bin/psql
PSQL     ?= psql
# 127.0.0.1, not localhost: podman publishes the port on IPv4 only, and a host that resolves
# localhost to ::1 first (the Linux rig does) gets a connection reset instead of a connection.
PSQLFLAGS = -h 127.0.0.1 -p 4566 -d dev -U root -v ON_ERROR_STOP=1

# A logged-out gcloud credential helper in ~/.docker/config.json aborts podman's credential
# lookup even for public registries; a bench-local empty auth file sidesteps it.
export REGISTRY_AUTH_FILE := $(CURDIR)/.auth.json

.PHONY: help info up down clean psql run smoke bless wait logs load-setup load rt-setup rt-load bench latency lat-report console metrics test

# Default target is deliberately inert: a bare `make` should not recreate a running cluster.
.DEFAULT_GOAL := help
help:
	@echo "usage: make <target>      (new here? run: make info)"
	@echo
	@echo "  # cluster"
	@echo "  up                       start the pinned image (recreates any leftover container)"
	@echo "  down                     stop and remove the container"
	@echo "  clean                    down, and drop the data volume"
	@echo "  wait                     block until pgwire answers on :4566"
	@echo "  logs                     follow container logs"
	@echo "  psql                     interactive session"
	@echo
	@echo "  # tests"
	@echo "  smoke                    run scenarios, assert against expected/"
	@echo "  bless                    re-record expected/"
	@echo "  run S=<file.sql>         run one scenario, echoing statements"
	@echo
	@echo "  # load and latency"
	@echo "  load-setup               create the bulk table and MV"
	@echo "  load [PROFILE=] [ROWS=]  feed and seal; PROFILE=small|fraud|hotspot"
	@echo "  bench [ROWS=] [RATE=] [ROUNDS=]"
	@echo "                           the whole realtime latency benchmark, both numbers"
	@echo "    rt-setup               ... build the pipeline"
	@echo "    rt-load [RATE=][ROWS=] ... background traffic"
	@echo "    latency [ROUNDS=]      ... [1] client probe: insert -> visible in mv_rt"
	@echo "    lat-report             ... [2] server-side: every match, incl. the sink hop"
	@echo
	@echo "  # console"
	@echo "  console [PIN=1] [PORT=]  the demo web console (cluster, load, feed, details tab)"
	@echo "  metrics                  operator counters straight off the compute node (:1222)"
	@echo "  test                     the Rust workspace's unit tests"
	@echo
	@echo "image: $(RW_IMAGE)"
	@echo "psql:  $(PSQL)   (override with PSQL=...)"

# Getting started, by intent rather than by target. `help` lists what exists; this says which
# three commands to type depending on why you are here.
info:
	@echo "This is a test bench for RisingWave builds carrying MATCH_RECOGNIZE."
	@echo "It runs a published image in podman and drives SQL against it. Nothing here"
	@echo "needs a RisingWave source tree."
	@echo
	@echo "Prerequisites"
	@echo "  podman        (this bench does not use docker)"
	@echo "  Rust >= 1.95  for the workload generator and the console"
	@echo "  psql          on a Mac with keg-only libpq:"
	@echo "                  export PSQL=/opt/homebrew/opt/libpq/bin/psql"
	@echo
	@echo "1. Demo it (the console drives the cluster itself — no 'make up' first)"
	@echo "     make console"
	@echo "   Open http://127.0.0.1:3000, then in the page: cluster up ->"
	@echo "   rebuild pipeline -> start load. The feed fills, the gauges move, and the"
	@echo "   details tab shows percentiles, operator counters and how trustworthy the"
	@echo "   host is. Add PIN=1 to partition the cores first."
	@echo
	@echo "2. Check a build is correct"
	@echo "     make up && make smoke"
	@echo "   Runs every scenario in scenarios/semantics/ and asserts the output against"
	@echo "   expected/. This is the gate to run against a newly published image."
	@echo
	@echo "3. Measure it"
	@echo "     make up && make bench                 # realtime: latency, both ways"
	@echo "     make load-setup && make load PROFILE=fraud   # bulk: ingest throughput"
	@echo "   PROFILE=small|fraud|hotspot; ROWS=, RATE=, ROUNDS= override the rest."
	@echo "   Numbers measured on Apple Silicon are emulated: shape-checks, not"
	@echo "   measurements. The console's details tab labels this for you."
	@echo
	@echo "When something looks wrong"
	@echo "  make logs        follow the container"
	@echo "  make metrics     operator counters straight off the compute node (:1222)"
	@echo "  make psql        poke at the data yourself"
	@echo "  make clean       drop the data volume — REQUIRED when switching to an image"
	@echo "                   with a different wire format, otherwise the new build aborts"
	@echo "                   during barrier recovery on the old state"
	@echo
	@echo "  make help        the full target list"

# The published images are linux/amd64 only; on Apple Silicon podman runs them emulated —
# fine for smoke and semantics runs, meaningless for performance numbers (use the rig).
# --replace so `make up` is idempotent: it recreates the container over a leftover one (stopped,
# or started earlier by compose) instead of failing with "name is already in use". The data volume
# is untouched, so this costs nothing but a restart -- use `make clean` to actually drop state.
up:
	podman run -d --replace --name $(NAME) --platform linux/amd64 \
		-p 4566:4566 -p 5690:5690 -p 1222:1222 \
		-v rw-tests-data:/root/.risingwave \
		$(RW_IMAGE) single_node
	$(MAKE) wait

# Bounded: a container that dies during startup (the classic case is barrier recovery aborting on
# state left by an incompatible tag — see the README) must fail the target, not hang forever.
WAIT_SECS ?= 180
wait:
	@echo "waiting for pgwire on :4566 (up to $(WAIT_SECS)s) ..."
	@i=0; until $(PSQL) $(PSQLFLAGS) -c "select 1" >/dev/null 2>&1; do \
		i=$$((i + 1)); \
		if [ $$i -ge $(WAIT_SECS) ]; then \
			echo "timed out after $(WAIT_SECS)s; last 40 log lines:" >&2; \
			podman logs --tail 40 $(NAME) >&2 || true; \
			exit 1; \
		fi; \
		if ! podman inspect -f '{{.State.Running}}' $(NAME) 2>/dev/null | grep -q true; then \
			echo "container $(NAME) is not running; last 40 log lines:" >&2; \
			podman logs --tail 40 $(NAME) >&2 || true; \
			exit 1; \
		fi; \
		sleep 1; \
	done
	@echo "ready"

down:
	podman rm -f $(NAME) 2>/dev/null || true

clean: down
	podman volume rm rw-tests-data 2>/dev/null || true

logs:
	podman logs -f $(NAME)

psql:
	$(PSQL) $(PSQLFLAGS)

# make run S=scenarios/semantics/preference_supersession.sql
run:
	$(PSQL) $(PSQLFLAGS) -e -f $(S)

# smoke asserts against recorded output in expected/ — semantics AND adversarial, since the
# backtracking probe is deterministic and self-contained. Without the recorded files a scenario
# only fails on a SQL error, and a silently wrong result set passes.
smoke:
	@PSQL=$(PSQL) scenarios/check.sh

# Re-record expected/*.out. Review the resulting diff against the expectations written in each
# script's comments before committing.
bless:
	@PSQL=$(PSQL) scenarios/check.sh --bless

# ---- Load & latency (real numbers belong on the rig; emulated runs are shape-checks only) ----
# Profiles: small (laptop shape-check), fraud (per-player keys, mild skew, open partials),
# hotspot (one partition dominating). Override any knob: make load PROFILE=fraud ROWS=2000000
PROFILE ?= small
ROWS    ?=
BENCH    = web/target/release/bench

$(BENCH):
	cd web && cargo build --release

ifeq ($(PROFILE),small)
GENARGS = --table t_perf --partitions 1000 --rows $(or $(ROWS),100000) --abandon-prob 0.2
else ifeq ($(PROFILE),fraud)
GENARGS = --table t_perf --partitions 100000 --rows $(or $(ROWS),1000000) --hot-count 100 --hot-share 0.3 --abandon-prob 0.25 --ties 2
else ifeq ($(PROFILE),hotspot)
GENARGS = --table t_perf --partitions 1000 --rows $(or $(ROWS),500000) --hot-count 1 --hot-share 0.9 --abandon-prob 0.3
endif

load-setup:
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_bulk.sql

# Feed, then seal. The seal is a separate step because a far-future sentinel delivered while the
# pipeline is still draining discards the in-flight rows instead of matching them.
load: $(BENCH)
	$(BENCH) load $(GENARGS)
	@$(BENCH) seal --table t_perf --mv mv_perf

rt-setup:
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_realtime.sql

rt-load: $(BENCH)
	$(BENCH) load --table t_rt --mode realtime --rate $(or $(RATE),2000) \
		--rows $(or $(ROWS),200000) --partitions 5000 --hot-count 5 --hot-share 0.4

# One command for the whole realtime benchmark: build the pipeline, run traffic, take both
# measurements, print them together. Everything below is the same run done by hand.
bench:
	@PSQL=$(PSQL) $(if $(ROWS),ROWS=$(ROWS)) $(if $(RATE),RATE=$(RATE)) $(if $(ROUNDS),ROUNDS=$(ROUNDS)) ./latency/bench.sh

# [1] client-side: insert, then poll mv_rt until the match appears.
latency:
	PSQL=$(PSQL) ROUNDS=$(or $(ROUNDS),10) ./latency/probe.sh

# [2] server-side: the proctime stamps every match recorded for itself, over the whole load.
lat-report:
	@$(PSQL) $(PSQLFLAGS) -f latency/report.sql

# ---- Console ---------------------------------------------------------------------------------
# The demo UI. It drives the cluster itself (podman up/down/clean), so `make up` is not a
# prerequisite — start the console and press "cluster up". PIN=1 partitions the cores; see the
# README's pinning section for what that does and does not buy on each platform.
CONSOLE = web/target/release/bench-web

$(CONSOLE):
	cd web && cargo build --release

console: $(CONSOLE)
	RW_IMAGE=$(RW_IMAGE) $(CONSOLE) --bind 127.0.0.1:$(or $(PORT),3000) $(if $(PIN),--pin)

# The three MATCH_RECOGNIZE counters, unaggregated, straight from the compute node's Prometheus
# endpoint — the same numbers the console's details tab sums. Useful without the UI, and the first
# thing to check if that tab shows dashes.
metrics:
	@curl -fsS localhost:1222/metrics | grep '^stream_match_recognize' \
		|| echo "no metrics on :1222 (is the cluster up, and is the port published?)"

test:
	cd web && cargo test --workspace
