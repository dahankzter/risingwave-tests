# Test bench for risingwave-mr images. Podman-only (this machine has no docker); a single
# container needs no compose.

RW_IMAGE ?= ghcr.io/dahankzter/risingwave:v3.1.0-alpha--mr--bee0fbd--feat-match-recognize-v2
NAME     ?= rw-tests
PSQL     ?= /opt/homebrew/opt/libpq/bin/psql
PSQLFLAGS = -h localhost -p 4566 -d dev -U root -v ON_ERROR_STOP=1

# A logged-out gcloud credential helper in ~/.docker/config.json aborts podman's credential
# lookup even for public registries; a bench-local empty auth file sidesteps it.
export REGISTRY_AUTH_FILE := $(CURDIR)/.auth.json

.PHONY: up down clean psql run smoke wait logs

# The published images are linux/amd64 only; on Apple Silicon podman runs them emulated —
# fine for smoke and semantics runs, meaningless for performance numbers (use the rig).
up:
	podman run -d --name $(NAME) --platform linux/amd64 \
		-p 4566:4566 -p 5690:5690 \
		-v rw-tests-data:/root/.risingwave \
		$(RW_IMAGE) single_node
	$(MAKE) wait

wait:
	@echo "waiting for pgwire on :4566 ..."
	@until $(PSQL) $(PSQLFLAGS) -c "select 1" >/dev/null 2>&1; do sleep 1; done
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

smoke:
	@for f in scenarios/semantics/*.sql; do \
		echo "=== $$f"; \
		$(PSQL) $(PSQLFLAGS) -f $$f || exit 1; \
	done
	@echo "smoke green"
