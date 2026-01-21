use clap::Parser;

/// Cli Arguments to test the query engine
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to the file of the data source
    #[arg(short, long)]
    pub file_path: String,

    /// Path to the file containing the SQL query
    #[arg(short, long)]
    pub query_path: String,
}
