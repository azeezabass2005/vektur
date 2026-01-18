use std::{ffi::OsStr, fs, path::Path};

use crate::{DataSource, Field, RecordBatch, errors::QueryError};

#[derive(Debug)]
pub struct CsvDataSource {
    file_path: String,
    original_schema: Vec<Field>,
}

impl CsvDataSource {
    pub fn new(file_path: String) -> Result<Self, QueryError> {
        let schema = Self::infer_schema(&file_path);

        match schema {
            Ok(schema) => {
                Ok(Self {
                    original_schema: schema,
                    file_path
                })
            },
            Err(err) => {
                Err(QueryError::DataSourceError { message: err })
            }
        }
    }
    pub fn infer_schema(file_path: &str) -> Result<Vec<Field>, String> {
        
        let path = Path::new(file_path);
        if !path.exists() || !path.is_file() {
            return Err("File doesn't exist".to_string());
        }
        match path.extension() {
            Some(ext) => {
                if ext != OsStr::new("csv") {
                    return Err("Please the file should be a csv file".to_string());
                };
            },
            None => {
                return Err("The path does not have an extension".to_string())
            }
        };

        let file = fs::read_to_string(path);
    
        match file {
            Ok(file) => {
                // Just printing the file to get familiar with it
                println!("{:?}", file);
                // I will still work on parsing the file here and getting the schema
                let fields: Vec<Field> =  Vec::new();
                Ok(fields)
            },
            Err(_) => {
                Err("Failed to read file to string".to_string())
            }
        }
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> &Vec<Field> {
        &self.original_schema
    }
    fn scan(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>> {
        todo!();
    }
}