# Test bench for risingwave-mr images. Podman-only (this machine has no docker); a single
# container needs no compose.

RW_IMAGE ?= ghcr.io/dahankzter/risingwave:v3.1.0-alpha--mr--bee0fbd--feat-match-recognize-v2
NAME     ?= rw-tests
# Prefer whatever is on PATH (the Linux rig); fall back to the Homebrew keg-only libpq, which a
# Mac does not put on PATH. Override with PSQL=... for anything else.
PSQL     ?= $(shell command -v psql 2>/dev/null || echo /opt/homebrew/opt/libpq/bin/psql)
# 127.0.0.1, not localhost: podman publishes the port on IPv4 only, and a host that resolves
# localhost to ::1 first (the Linux rig does) gets a connection reset instead of a connection.
PSQLFLAGS = -h 127.0.0.1 -p 4566 -d dev -U root -v ON_ERROR_STOP=1

# A logged-out gcloud credential helper in ~/.docker/config.json aborts podman's credential
# lookup even for public registries; a bench-local empty auth file sidesteps it.
export REGISTRY_AUTH_FILE := $(CURDIR)/.auth.json

.PHONY: up down clean psql run smoke bless wait logs load-setup load rt-setup rt-load latency

# The published images are linux/amd64 only; on Apple Silicon podman runs them emulated —
# fine for smoke and semantics runs, meaningless for performance numbers (use the rig).
up:
	podman run -d --name $(NAME) --platform linux/amd64 \
		-p 4566:4566 -p 5690:5690 \
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
GEN      = python3 datagen/gen.py

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
# pipeline is still draining discards the in-flight rows instead of matching them -- see
# datagen/seal.sh.
load:
	$(GEN) $(GENARGS) | $(PSQL) $(PSQLFLAGS) -q
	@PSQL=$(PSQL) TABLE=t_perf MV=mv_perf ./datagen/seal.sh

rt-setup:
	$(PSQL) $(PSQLFLAGS) -f scenarios/perf/setup_realtime.sql

rt-load:
	$(GEN) --table t_rt --mode realtime --rate $(or $(RATE),2000) --rows $(or $(ROWS),200000) --partitions 5000 --hot-count 5 --hot-share 0.4 | $(PSQL) $(PSQLFLAGS) -q

latency:
	ROUNDS=$(or $(ROUNDS),10) ./latency/probe.sh
