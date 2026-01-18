use clap::Parser;

/// Cli Arguments to test the query engine
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Type of the data source wether csv or parquet (defaults to csv for now)
    // #[arg(short, long)]
    // pub source: String,

    /// Path to the file of the data source
    #[arg(short, long)]
    pub file_path: String,
}
