# risingwave-tests

A personal test bench for the MATCH_RECOGNIZE contribution to RisingWave
([risingwavelabs/risingwave#26584](https://github.com/risingwavelabs/risingwave/pull/26584)):
scenario scripts, adversarial patterns, and performance probes run against published
`risingwave-mr` images.

**Identity note:** this repo belongs to the personal open-source track (`dahankzter`). Nothing
work-related lands here, and nothing here references work systems or data.

## Usage

```sh
make up                # start a single-node RisingWave from $RW_IMAGE (podman)
make psql              # interactive session on :4566
make run S=scenarios/semantics/preference_supersession.sql
make smoke             # run every scenario under scenarios/semantics/
make down              # stop; make clean also removes the data volume
```

The image is pinned in the `RW_IMAGE` variable (Makefile / compose environment). Until the next
image is published it points at a placeholder tag — override per invocation:

```sh
make up RW_IMAGE=ghcr.io/dahankzter/risingwave-mr:0.2.0
```

## Layout

- `scenarios/semantics/` — cluster-level checks of matching semantics: preference supersession,
  PERMUTE listing order, `EMIT ON WINDOW CLOSE` clause acceptance. These mirror the upstream SLTs
  but run against a released image rather than a source build.
- `scenarios/adversarial/` — patterns designed to hurt: catastrophic-backtracking shapes, scan
  budget probes.
- `scenarios/perf/` — partition-cardinality sweeps, `WITHIN` retention soak, throughput probes,
  fed by `datagen/`.
- `datagen/` — parameterized generators that pipe `INSERT`s through psql.

Scenario scripts are plain psql SQL files. Expected results are stated in comments at the point of
each query; scripts are written to be re-runnable (they drop and recreate their own objects).
