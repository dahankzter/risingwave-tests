-- PERMUTE preference: PERMUTE(a, b) expands to (a b | b a) in listing order; when the data
-- satisfies both orderings, the first-listed must win. CLASSIFIER() distinguishes them where a
-- count cannot.

set rw_implicit_flush to true;
set streaming_parallelism = 1;

drop materialized view if exists mv_permute_pref;
drop table if exists t_perm;

create table t_perm (id int, ts int, v int, watermark for ts as ts - 1) append only;

create materialized view mv_permute_pref as
select * from t_perm match_recognize (
  partition by id
  order by ts
  measures classifier() as last_var
  one row per match
  after match skip past last row
  pattern (permute(a, b))
  define a as a.v > 0, b as b.v < 10
);

-- Both rows satisfy both variables (0 < 5 < 10); the (a b) ordering is preferred, so the last
-- row classifies as b.
insert into t_perm values (1, 10, 5), (1, 11, 5), (1, 12, 0), (9, 14, 5);
\echo 'expect: (1, b)'
select * from mv_permute_pref;

drop materialized view mv_permute_pref;
drop table t_perm;
