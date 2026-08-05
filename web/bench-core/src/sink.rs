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

impl<W: Write> EmitSql<W> {
    pub fn write(&mut self, rows: &[Row]) -> anyhow::Result<()> {
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

    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.out.flush()?;
        Ok(())
    }
}

/// A real connection. Rows are bound as parameters rather than formatted into SQL text.
pub struct Direct {
    client: tokio_postgres::Client,
    table: String,
    columns: String,
    payload_cols: usize,
}

impl Direct {
    pub async fn connect(
        url: &str,
        table: String,
        payload_cols: usize,
    ) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
        // The connection future drives the protocol and must be polled for the client to work.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("bench: connection closed: {e}");
            }
        });
        let columns = column_list(payload_cols);
        Ok(Self { client, table, columns, payload_cols })
    }

    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub async fn write_async(&mut self, rows: &[Row]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let per_row = 4 + self.payload_cols;
        let mut sql = format!("insert into {} {} values ", self.table, self.columns);
        for i in 0..rows.len() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for c in 0..per_row {
                if c > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("${}", i * per_row + c + 1));
            }
            sql.push(')');
        }

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            Vec::with_capacity(rows.len() * per_row);
        for r in rows {
            params.push(Box::new(r.partition));
            match r.ts {
                Ts::Tick(t) => {
                    let t32 = i32::try_from(t).map_err(|_| {
                        anyhow::anyhow!(
                            "tick {t} does not fit in the `ts` column's int4 type \
                             (range {}..={}); a bulk feed this long needs a wider `ts` column \
                             or a lower row count",
                            i32::MIN,
                            i32::MAX
                        )
                    })?;
                    params.push(Box::new(t32));
                }
                Ts::Wall(t) => params.push(Box::new(t)),
            }
            params.push(Box::new(r.kind.as_str().to_string()));
            params.push(Box::new(r.amount));
            for p in &r.payload {
                params.push(Box::new(p.clone()));
            }
        }
        let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();

        self.client.execute(&sql, &refs).await?;
        Ok(())
    }
}

/// The two places rows can go, unified behind one owned value instead of two parallel
/// `Option`s. `Direct::write_async` is async and `EmitSql::write` is sync (it never does I/O
/// that would block), so this dispatches rather than being a trait: a trait here would need
/// `Direct` and `EmitSql` to share one method signature, and the only signature that fits both
/// is async — which would make every `EmitSql` write pay for an executor it never needs.
///
/// The `Emit` variant boxes its writer (`Box<dyn Write + Send>`) instead of the enum taking a
/// `W: Write` type parameter — a generic enum would push that parameter onto every caller
/// (`RunConfig`, `RunHandle`, ...) for a path that writes in batches of a few hundred rows at a
/// time, where one dynamic dispatch per batch is not worth it.
pub enum Sink {
    Direct(Direct),
    Emit(EmitSql<Box<dyn Write + Send>>),
}

impl Sink {
    pub async fn write(&mut self, rows: &[Row]) -> anyhow::Result<()> {
        match self {
            Sink::Direct(d) => d.write_async(rows).await,
            Sink::Emit(e) => e.write(rows),
        }
    }

    pub async fn finish(&mut self) -> anyhow::Result<()> {
        match self {
            Sink::Direct(_) => Ok(()),
            Sink::Emit(e) => e.finish(),
        }
    }
}
