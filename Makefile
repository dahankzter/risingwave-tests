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

.PHONY: help info doctor check-podman check-rust check-psql check-python
.PHONY: up down clean psql run smoke bless wait logs load-setup load rt-setup rt-load
.PHONY: bench latency lat-report latency-model console metrics test

# Default target is deliberately inert: a bare `make` should not recreate a running cluster.
.DEFAULT_GOAL := help
help:
	@echo "usage: make <target>      (new here? run: make info; setup trouble? make doctor)"
	@echo
	@echo "  # setup"
	@echo "  doctor                   check podman, rust, psql and python3, with install hints"
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
	@echo "  latency-model [REPEAT=]  why a result is visible when it is: barrier vs watermark"
	@echo
	@echo "  # console"
	@echo "  console [PIN=1] [PORT=]  the demo web console (cluster, load, feed, details tab)"
	@echo "  metrics                  operator counters straight off the compute node (:1260)"
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
	@echo "Prerequisites  ->  run 'make doctor' to check all four at once, with the"
	@echo "                   install command for this machine's package manager"
	@echo "  podman        (this bench does not use docker)"
	@echo "  Rust >= $(RUST_MIN)  for the workload generator and the console"
	@echo "  psql          on a Mac with keg-only libpq:"
	@echo "                  export PSQL=/opt/homebrew/opt/libpq/bin/psql"
	@echo "  python3       for 'make latency-model' only"
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
	@echo "  make metrics     operator counters straight off the compute node (:1260)"
	@echo "  make psql        poke at the data yourself"
	@echo "  make clean       drop the data volume — REQUIRED when switching to an image"
	@echo "                   with a different wire format, otherwise the new build aborts"
	@echo "                   during barrier recovery on the old state"
	@echo
	@echo "  make help        the full target list"

# ---- Prerequisites -------------------------------------------------------------------------------
# A missing tool should print a command that works on the machine reading the message, so the
# package manager is detected rather than listed. `make doctor` runs every check at once.
#
# Detection happens at parse time into single-line variables on purpose: a multi-line `define`
# expanded inside a recipe is one of make's better traps, and this needs to be the code that works
# when nothing else does.

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
  PM := brew
else
  # First manager found wins; covers the mainstream distros without pretending to cover all of them.
  PM := $(shell for m in dnf pacman apt-get zypper apk; do \
          command -v $$m >/dev/null 2>&1 && echo $$m && break; done)
endif

PM_CMD_brew    := brew install
PM_CMD_dnf     := sudo dnf install
PM_CMD_pacman  := sudo pacman -S
PM_CMD_apt-get := sudo apt install
PM_CMD_zypper  := sudo zypper install
PM_CMD_apk     := sudo apk add
PM_CMD         := $(PM_CMD_$(PM))

# Package names differ enough between managers to be worth spelling out rather than guessing.
PKG_PODMAN_brew    := podman
PKG_PODMAN_dnf     := podman
PKG_PODMAN_pacman  := podman
PKG_PODMAN_apt-get := podman
PKG_PODMAN_zypper  := podman
PKG_PODMAN_apk     := podman
PKG_PODMAN         := $(PKG_PODMAN_$(PM))

PKG_PY_brew    := python
PKG_PY_dnf     := python3
PKG_PY_pacman  := python
PKG_PY_apt-get := python3
PKG_PY_zypper  := python3
PKG_PY_apk     := python3
PKG_PY         := $(PKG_PY_$(PM))

# psql comes from the client package, not the server. Homebrew's libpq is keg-only, which is why
# PSQL exists as a variable at the top of this file.
PKG_PSQL_brew    := libpq
PKG_PSQL_dnf     := postgresql
PKG_PSQL_pacman  := postgresql-libs
PKG_PSQL_apt-get := postgresql-client
PKG_PSQL_zypper  := postgresql
PKG_PSQL_apk     := postgresql-client
PKG_PSQL         := $(PKG_PSQL_$(PM))

# The minimum toolchain is read from the workspace rather than duplicated, so bumping
# web/Cargo.toml's rust-version cannot leave this check behind vouching for an older one.
RUST_MIN := $(shell sed -n 's/^rust-version = "\([0-9.]*\)".*/\1/p' web/Cargo.toml)

# Printed when the manager is unknown, or as the "other distros" footnote.
define OTHER_DISTROS
	echo "    Fedora/RHEL:    sudo dnf install $(1)"; \
	echo "    Arch:           sudo pacman -S $(2)"; \
	echo "    Debian/Ubuntu:  sudo apt install $(3)"; \
	echo "    openSUSE:       sudo zypper install $(4)";
endef

check-podman:
	@command -v podman >/dev/null 2>&1 || { \
		echo "" >&2; \
		echo "podman is not installed." >&2; \
		echo "This bench is podman-only: it runs one container directly, with no compose and no" >&2; \
		echo "docker. Install it:" >&2; \
		echo "" >&2; \
		if [ -n "$(PM_CMD)" ]; then echo "    $(PM_CMD) $(PKG_PODMAN)" >&2; else \
			$(call OTHER_DISTROS,podman,podman,podman,podman) >&2; fi; \
		if [ "$(UNAME_S)" = "Darwin" ]; then \
			echo "" >&2; \
			echo "Then start its VM — on macOS podman needs one, and every command fails" >&2; \
			echo "confusingly until it is running:" >&2; \
			echo "" >&2; \
			echo "    podman machine init" >&2; \
			echo "    podman machine start" >&2; \
		fi; \
		echo "" >&2; \
		exit 1; \
	}
	@podman info >/dev/null 2>&1 || { \
		echo "" >&2; \
		echo "podman is installed but not responding." >&2; \
		if [ "$(UNAME_S)" = "Darwin" ]; then \
			echo "On macOS this almost always means its VM is not running:" >&2; \
			echo "" >&2; \
			echo "    podman machine start        # or: podman machine init, the first time" >&2; \
		else \
			echo "Check the service and your permissions:" >&2; \
			echo "" >&2; \
			echo "    systemctl --user start podman.socket" >&2; \
			echo "    podman info                 # for the underlying error" >&2; \
		fi; \
		echo "" >&2; \
		exit 1; \
	}

check-rust:
	@command -v cargo >/dev/null 2>&1 || { \
		echo "" >&2; \
		echo "cargo is not installed; the bench tools and the web console are Rust." >&2; \
		echo "" >&2; \
		echo "    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh" >&2; \
		echo "" >&2; \
		echo "rustup rather than a distro package deliberately: this workspace requires Rust" >&2; \
		echo "$(RUST_MIN) or newer, and distro toolchains are often older." >&2; \
		echo "" >&2; \
		exit 1; \
	}
	@have=$$(cargo --version 2>/dev/null | awk '{print $$2}'); \
	oldest=$$(printf '%s\n%s\n' "$(RUST_MIN)" "$$have" | sort -V | head -1); \
	if [ "$$oldest" != "$(RUST_MIN)" ]; then \
		echo "" >&2; \
		echo "cargo $$have is too old: this workspace requires $(RUST_MIN) or newer" >&2; \
		echo "(web/Cargo.toml sets rust-version, and this check reads it from there)." >&2; \
		echo "" >&2; \
		if command -v rustup >/dev/null 2>&1; then \
			echo "    rustup update stable" >&2; \
		else \
			echo "    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh" >&2; \
			echo "" >&2; \
			echo "That cargo came from somewhere other than rustup; installing rustup gives you" >&2; \
			echo "a toolchain you can keep current." >&2; \
		fi; \
		echo "" >&2; \
		exit 1; \
	fi

check-psql:
	@command -v $(PSQL) >/dev/null 2>&1 || { \
		echo "" >&2; \
		echo "psql not found (looked for: $(PSQL))." >&2; \
		echo "" >&2; \
		if [ -n "$(PM_CMD)" ]; then echo "    $(PM_CMD) $(PKG_PSQL)" >&2; else \
			$(call OTHER_DISTROS,postgresql,postgresql-libs,postgresql-client,postgresql) >&2; fi; \
		if [ "$(UNAME_S)" = "Darwin" ]; then \
			echo "" >&2; \
			echo "Homebrew keeps libpq keg-only, so installing it does not put psql on PATH." >&2; \
			echo "Point this bench at it instead:" >&2; \
			echo "" >&2; \
			echo "    export PSQL=/opt/homebrew/opt/libpq/bin/psql" >&2; \
		fi; \
		echo "" >&2; \
		exit 1; \
	}

check-python:
	@command -v python3 >/dev/null 2>&1 || { \
		echo "" >&2; \
		echo "python3 not found; the latency-model harness is a stdlib-only python script." >&2; \
		echo "" >&2; \
		if [ -n "$(PM_CMD)" ]; then echo "    $(PM_CMD) $(PKG_PY)" >&2; else \
			$(call OTHER_DISTROS,python3,python,python3,python3) >&2; fi; \
		echo "" >&2; \
		exit 1; \
	}

# Every check at once, reporting all findings rather than stopping at the first — someone setting a
# machine up wants the whole list, not one item per attempt.
doctor:
	@echo "platform:         $(UNAME_S)"
	@echo "package manager:  $(if $(PM),$(PM),none recognised)"
	@echo "required rust:    $(RUST_MIN) or newer"
	@echo
	@fail=0; \
	for c in podman rust psql python; do \
		if $(MAKE) --no-print-directory check-$$c >/dev/null 2>&1; then \
			printf '  ok    %s\n' "$$c"; \
		else \
			printf '  MISSING %s\n' "$$c"; fail=1; \
		fi; \
	done; \
	echo; \
	if [ $$fail -eq 0 ]; then \
		echo "all prerequisites present — run: make info"; \
	else \
		echo "for the fix, run the individual check, e.g.:  make check-podman"; \
		exit 1; \
	fi

# The published images are linux/amd64 only; on Apple Silicon podman runs them emulated —
# fine for smoke and semantics runs, meaningless for performance numbers (use the rig).
# --replace so `make up` is idempotent: it recreates the container over a leftover one (stopped,
# or started earlier by compose) instead of failing with "name is already in use". The data volume
# is untouched, so this costs nothing but a restart -- use `make clean` to actually drop state.
up: check-podman
	podman run -d --replace --name $(NAME) --platform linux/amd64 \
		-p 4566:4566 -p 5690:5690 -p 1260:1260 \
		-v rw-tests-data:/root/.risingwave \
		$(RW_IMAGE) single_node --prometheus-listener-addr 0.0.0.0:1260
	$(MAKE) wait

# Bounded: a container that dies during startup (the classic case is barrier recovery aborting on
# state left by an incompatible tag — see the README) must fail the target, not hang forever.
WAIT_SECS ?= 180
wait: check-podman check-psql
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

down: check-podman
	podman rm -f $(NAME) 2>/dev/null || true

clean: check-podman down
	podman volume rm rw-tests-data 2>/dev/null || true

logs: check-podman
	podman logs -f $(NAME)

psql: check-psql
	$(PSQL) $(PSQLFLAGS)

# make run S=scenarios/semantics/preference_supersession.sql
run: check-psql
	$(PSQL) $(PSQLFLAGS) -e -f $(S)

# smoke asserts against recorded output in expected/ — semantics AND adversarial, since the
# backtracking probe is deterministic and self-contained. Without the recorded files a scenario
# only fails on a SQL error, and a silently wrong result set passes.
smoke: check-psql
	@PSQL=$(PSQL) scenarios/check.sh

# Re-record expected/*.out. Review the resulting diff against the expectations written in each
# script's comments before committing.
bless: check-psql
	@PSQL=$(PSQL) scenarios/check.sh --bless

# ---- Load & latency (real numbers belong on the rig; emulated runs are shape-checks only) ----
# Profiles: small (laptop shape-check), fraud (per-player keys, mild skew, open partials),
# hotspot (one partition dominating). Override any knob: make load PROFILE=fraud ROWS=2000000
PROFILE ?= small
ROWS    ?=
BENCH    = web/target/release/bench

$(BENCH): check-rust
	cd web && cargo build --release

ifeq ($(PROFILE),small)
GENARGS = --table t_perf --partitions 1000 --rows $(or $(ROWS),100000) --abandon-prob 0.2
else ifeq ($(PROFILE),fraud)
GENARGS = --table t_perf --partitions 100000 --rows $(or $(ROWS),1000000) --hot-count 100 --hot-share 0.3 --abandon-prob 0.25 --ties 2
else ifeq ($(PROFILE),hotspot)
GENARGS = --table t_perf --partitions 1000 --rows $(or $(ROWS),500000) --hot-count 1 --hot-share 0.9 --abandon-prob 0.3
endif

load-setup: check-psql
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_bulk.sql

# Feed, then seal. The seal is a separate step because a far-future sentinel delivered while the
# pipeline is still draining discards the in-flight rows instead of matching them.
load: $(BENCH)
	$(BENCH) load $(GENARGS)
	@$(BENCH) seal --table t_perf --mv mv_perf

rt-setup: check-psql
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_realtime.sql

rt-load: $(BENCH)
	$(BENCH) load --table t_rt --mode realtime --rate $(or $(RATE),2000) \
		--rows $(or $(ROWS),200000) --partitions 5000 --hot-count 5 --hot-share 0.4

# One command for the whole realtime benchmark: build the pipeline, run traffic, take both
# measurements, print them together. Everything below is the same run done by hand.
bench: check-psql
	@PSQL=$(PSQL) $(if $(ROWS),ROWS=$(ROWS)) $(if $(RATE),RATE=$(RATE)) $(if $(ROUNDS),ROUNDS=$(ROUNDS)) ./latency/bench.sh

# [1] client-side: insert, then poll mv_rt until the match appears.
latency: check-psql
	PSQL=$(PSQL) ROUNDS=$(or $(ROUNDS),10) ./latency/probe.sh

# The two clocks that decide when a result becomes visible: barrier-gated vs watermark-gated, plus
# the starvation case where a quiet stream stops emitting entirely. Produces the table in
# docs/latency-model.md. Needs the console running (`make console`) and nothing else installed.
latency-model: check-python
	python3 scenarios/perf/latency_model.py \
	  --console http://127.0.0.1:$(or $(PORT),3000) \
	  --repeat $(or $(REPEAT),5)

# [2] server-side: the proctime stamps every match recorded for itself, over the whole load.
lat-report: check-psql
	@$(PSQL) $(PSQLFLAGS) -f latency/report.sql

# ---- Console ---------------------------------------------------------------------------------
# The demo UI. It drives the cluster itself (podman up/down/clean), so `make up` is not a
# prerequisite — start the console and press "cluster up". PIN=1 partitions the cores; see the
# README's pinning section for what that does and does not buy on each platform.
CONSOLE = web/target/release/bench-web

$(CONSOLE): check-rust
	cd web && cargo build --release

console: $(CONSOLE)
	RW_IMAGE=$(RW_IMAGE) $(CONSOLE) --bind 127.0.0.1:$(or $(PORT),3000) $(if $(PIN),--pin)

# The three MATCH_RECOGNIZE counters, unaggregated, straight from the compute node's Prometheus
# endpoint — the same numbers the console's details tab sums. Useful without the UI, and the first
# thing to check if that tab shows dashes.
metrics:
	@curl -fsS localhost:1260/metrics | grep '^stream_match_recognize' \
		|| echo "no metrics on :1260 (is the cluster up, and is the port published?)"

test: check-rust
	cd web && cargo test --workspace
