use anyhow::{Context, Result};
use arrow_array::{Array, RecordBatch, StringArray};
use clap::Parser;
use kq::loader::{LoaderConfig, SnapshotData, SnapshotLoader};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "kq-snapshot-correctness",
    about = "Compare two snapshot path sets for schema, row-count, and key aggregate equality"
)]
struct Args {
    /// Expected/original snapshot path. Repeat once per snapshot.
    #[arg(long = "expected", value_name = "SNAPSHOT", required = true)]
    expected: Vec<PathBuf>,

    /// Actual/optimized snapshot path. Repeat once per snapshot.
    #[arg(long = "actual", value_name = "SNAPSHOT", required = true)]
    actual: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let expected = load(&args.expected).await.context("failed to load expected snapshots")?;
    let actual = load(&args.actual).await.context("failed to load actual snapshots")?;

    compare_tables(&expected, &actual)?;
    compare_string_counts(&expected, &actual, "pods", "cluster")?;
    compare_string_counts(&expected, &actual, "pods", "phase")?;
    compare_string_counts(&expected, &actual, "pods", "namespace")?;
    compare_string_counts(&expected, &actual, "pods", "app")?;
    compare_string_counts(&expected, &actual, "nodes", "cluster")?;

    println!("Correctness validation passed");
    for table in expected.list_tables() {
        println!(
            "  {}: {} rows",
            table,
            expected.table_row_count(&table)
        );
    }

    Ok(())
}

async fn load(paths: &[PathBuf]) -> Result<SnapshotData> {
    let config = LoaderConfig {
        progress_updates: false,
        ..Default::default()
    };
    SnapshotLoader::with_config(config).load_and_combine(paths).await
}

fn compare_tables(expected: &SnapshotData, actual: &SnapshotData) -> Result<()> {
    let expected_tables = expected.list_tables();
    let actual_tables = actual.list_tables();
    if expected_tables != actual_tables {
        anyhow::bail!(
            "table names differ: expected {:?}, actual {:?}",
            expected_tables,
            actual_tables
        );
    }

    for table in expected_tables {
        let expected_rows = expected.table_row_count(&table);
        let actual_rows = actual.table_row_count(&table);
        if expected_rows != actual_rows {
            anyhow::bail!(
                "row count differs for {}: expected {}, actual {}",
                table,
                expected_rows,
                actual_rows
            );
        }

        let expected_schema = expected
            .table_schema(&table)
            .ok_or_else(|| anyhow::anyhow!("missing expected schema for {}", table))?;
        let actual_schema = actual
            .table_schema(&table)
            .ok_or_else(|| anyhow::anyhow!("missing actual schema for {}", table))?;
        if expected_schema.fields() != actual_schema.fields() {
            anyhow::bail!("schema fields differ for {}", table);
        }
    }

    Ok(())
}

fn compare_string_counts(
    expected: &SnapshotData,
    actual: &SnapshotData,
    table: &str,
    column: &str,
) -> Result<()> {
    let expected_counts = string_counts(expected, table, column)?;
    let actual_counts = string_counts(actual, table, column)?;
    if expected_counts != actual_counts {
        anyhow::bail!(
            "aggregate counts differ for {}.{}: expected {:?}, actual {:?}",
            table,
            column,
            expected_counts,
            actual_counts
        );
    }
    Ok(())
}

fn string_counts(data: &SnapshotData, table: &str, column: &str) -> Result<BTreeMap<String, usize>> {
    let batches = data
        .get_table_batches(table)
        .ok_or_else(|| anyhow::anyhow!("missing table batches for {}", table))?;
    let mut counts = BTreeMap::new();

    for batch in batches {
        count_batch_strings(batch, table, column, &mut counts)?;
    }

    Ok(counts)
}

fn count_batch_strings(
    batch: &RecordBatch,
    table: &str,
    column: &str,
    counts: &mut BTreeMap<String, usize>,
) -> Result<()> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| anyhow::anyhow!("missing column {}.{}", table, column))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("column {}.{} is not a string array", table, column))?;

    for row in 0..array.len() {
        let key = if array.is_null(row) {
            "<null>".to_string()
        } else {
            array.value(row).to_string()
        };
        *counts.entry(key).or_insert(0) += 1;
    }

    Ok(())
}

