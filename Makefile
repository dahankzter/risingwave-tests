# Test bench for risingwave-mr images. Podman-only (this machine has no docker).

RW_IMAGE ?= ghcr.io/dahankzter/risingwave:v3.1.0-alpha--mr--0bc2acb--feat-match-recognize-v2
PSQL     ?= /opt/homebrew/opt/libpq/bin/psql
PSQLFLAGS = -h localhost -p 4566 -d dev -U root -v ON_ERROR_STOP=1

export RW_IMAGE

.PHONY: up down clean psql run smoke wait

up:
	podman compose up -d
	$(MAKE) wait

wait:
	@echo "waiting for pgwire on :4566 ..."
	@until $(PSQL) $(PSQLFLAGS) -c "select 1" >/dev/null 2>&1; do sleep 1; done
	@echo "ready"

down:
	podman compose down

clean:
	podman compose down -v

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
