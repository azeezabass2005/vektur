use clap::Parser;
use vektur::{cli::arg::Args, datasource::csv::CsvDataSource};


fn main() {
    let args = Args::parse();
    let data_source = CsvDataSource::new(args.file_path);
    match data_source {
        Ok(source) => {
            println!("{:?}", source);
        },
        Err(err) => {
            println!("An error occurred: {:?}", err);
        }
    };
}
