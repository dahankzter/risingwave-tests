# Playground demos

Four runnable tours of MATCH_RECOGNIZE, from a plain aggregate to two matchers stacked on each
other. Each file is self-contained — its own objects, its own drops, its own data — so they can be
run in any order, repeatedly, without tripping over each other.

Two ways to use them:

- **From the console's correctness tab**, under *demos*. They carry `\echo 'expect: …'` lines, so
  each result arrives captioned with what it should show.
- **Pasted into the playground tab**, a statement or a whole file at a time, to change a clause and
  watch what happens to the output. That is the point of having them as text.

Unlike the semantics checks in `../semantics`, these deliberately **leave their tables and views
behind**. Run one and switch to the playground: the objects are waiting, and the catalog re-reads
itself on arrival. Rerunning a file drops its own objects first, so nothing accumulates.

| file | shows | takes |
|---|---|---|
| `01_streaming_basics.sql` | a watermarked append-only table and a maintained aggregate; `[append_only]` in the plan | ~3s |
| `02_match_recognize.sql` | `pattern (d b* w) within interval '60' second`; a quantifier matching *zero* times | ~11s |
| `03_skip_modes.sql` | the same pattern under two `AFTER MATCH SKIP` modes — 1 row vs 3 — then an aggregate over match output | ~13s |
| `04_match_over_match.sql` | two matchers stacked, bridged by a sink into a watermarked table | ~20s |

## Things that cost time to discover

**`count(*)` and `first_value()` do not work in MEASURES.** `count(b.*)` reports *"COUNT() argument
must be a pattern-variable column"*, and `first_value(a.amount)` reports *"AggCall first_value($0)
has not been rewritten to physical aggregate operators"*. Use `count(b.amount)` and a plain
`a.amount`.

**Sink syntax is `into <table> as <query>`.** The `from` form takes a relation name, not a query:
`create sink s into t from mv` is fine, `... from select …` is a parse error.

**A match emits once the watermark passes its end**, which is why every file ends its data with a
sentinel row beyond the batch. Keep the sentinel just past its own batch — one far in the future
makes every *later* insert late, and late rows are dropped silently. That cost an afternoon once.

**`flush` does not cover a sink hop.** It waits for a checkpoint on the DML path, so it drains an
insert deterministically, but a table fed by a *sink* is not on that path. `04` therefore needs a
real wait, and it is the slowest thing here to become visible. On a loaded machine its last read may
still come back empty — rerun that select, or read `pg_l2` in the playground.

**`select * from <sink>` fails** with a bare "Failed to prepare the statement" and nothing under it.
That is why the playground's **show data** button greys out on a sink.

The settle times above were measured by running each file three or four times in a row; the sleeps
carry margin over the shortest interval that worked, because the first values chosen passed once and
then failed twice.
