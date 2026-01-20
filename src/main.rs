use clap::Parser;
use vektur::{DataSource, cli::arg::Args, datasource::csv::CsvDataSource};


fn main() {
    let args = Args::parse();
    let data_source = CsvDataSource::new(args.file_path);
    match data_source {
        Ok(source) => {
            // println!("{:?}", source);
            for data in source.scan() {
                match data {
                    Ok(batch) => {
                        println!("This is the batch {:?}", batch)
                    },
                    Err(err) => {
                        println!("This error occurred while batching: {:?}", err)
                    }
                }
            }
        },
        Err(err) => {
            println!("An error occurred: {:?}", err);
        }
    };
}
