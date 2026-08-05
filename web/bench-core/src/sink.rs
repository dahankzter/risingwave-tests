//! Where generated rows go.
//!
//! The generator emits typed `Row`s; only this module turns them into SQL. `Direct` binds them as
//! parameters against a real connection, `EmitSql` formats them for inspection. Keeping the
//! generator ignorant of SQL text removes quoting bugs by construction for the generator-controlled
//! fields (`Kind`, and the numeric fields, which cannot contain a quote). `payload` is a free-form
//! `Vec<String>`, so its values are not exempt from quoting concerns; they are escaped on the way
//! out by doubling any single quote, the standard SQL string-literal escape.

use crate::gen::Kind;
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum Ts {
    /// Bulk mode: integer ticks. Deterministic, and WITHIN bounds are expressed in ticks.
    Tick(i64),
    /// Realtime mode: wall clock, taken at insert time.
    Wall(time::OffsetDateTime),
}

#[derive(Debug, Clone)]
pub struct Row {
    pub partition: i32,
    pub ts: Ts,
    pub kind: Kind,
    pub amount: i32,
    pub payload: Vec<String>,
}

/// Explicit column list. Required because the realtime table has a generated `ingest_ts` column,
/// so positional inserts do not line up.
pub fn column_list(payload_cols: usize) -> String {
    let mut s = String::from("(id, ts, kind, amount");
    for i in 0..payload_cols {
        s.push_str(&format!(", p{i}"));
    }
    s.push(')');
    s
}

/// Escape a payload value as a SQL string literal by doubling any single quote, the standard SQL
/// escape. `Kind` and the numeric fields never need this: they come from a fixed enum and integer
/// types respectively, but `payload` is free-form and generator-supplied callers can put anything in
/// it.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub trait Sink {
    fn write(&mut self, rows: &[Row]) -> anyhow::Result<()>;
    fn finish(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct EmitSql<W: Write> {
    out: W,
    table: String,
    columns: String,
}

impl<W: Write> EmitSql<W> {
    pub fn new(out: W, table: String, payload_cols: usize) -> Self {
        let columns = column_list(payload_cols);
        Self { out, table, columns }
    }

    fn ts_literal(ts: &Ts) -> String {
        match ts {
            Ts::Tick(t) => t.to_string(),
            Ts::Wall(t) => {
                let fmt = time::format_description::well_known::Rfc3339;
                format!("'{}'", t.format(&fmt).expect("rfc3339 formatting"))
            }
        }
    }
}

impl<W: Write> Sink for EmitSql<W> {
    fn write(&mut self, rows: &[Row]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        write!(self.out, "insert into {} {} values ", self.table, self.columns)?;
        for (i, r) in rows.iter().enumerate() {
            if i > 0 {
                write!(self.out, ", ")?;
            }
            write!(
                self.out,
                "({}, {}, '{}', {}",
                r.partition,
                Self::ts_literal(&r.ts),
                r.kind.as_str(),
                r.amount
            )?;
            for p in &r.payload {
                write!(self.out, ", {}", quote_literal(p))?;
            }
            write!(self.out, ")")?;
        }
        writeln!(self.out, ";")?;
        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.out.flush()?;
        Ok(())
    }
}
