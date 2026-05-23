mod snapshot_convert_common;

use anyhow::Result;
use clap::Parser;
use snapshot_convert_common::{convert_snapshot, ConvertOptions, SnapshotOutputFormat};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "kq-snapshot-to-ipc",
    about = "Convert a kq NDJSON snapshot directory into a per-table Arrow IPC snapshot directory"
)]
struct Args {
    /// Input NDJSON snapshot directory
    #[arg(short, long, value_name = "DIR")]
    input: PathBuf,

    /// Output Arrow IPC snapshot directory
    #[arg(short, long, value_name = "DIR")]
    output: PathBuf,

    /// Replace an existing output directory
    #[arg(long)]
    overwrite: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    convert_snapshot(ConvertOptions {
        input: args.input,
        output: args.output,
        overwrite: args.overwrite,
        format: SnapshotOutputFormat::Ipc,
    })
}
