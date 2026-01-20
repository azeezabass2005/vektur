use std::{ffi::{OsString}, fs::File, io::{BufRead, BufReader}, path::{Path, PathBuf}};

use crate::{DataSource, DataType, Field, RecordBatch, errors::QueryError};

#[derive(Debug)]
pub struct ValidCsvPath(PathBuf);

impl ValidCsvPath {
    pub fn new(path: &str) -> Result<Self, QueryError> {
        let path = Path::new(path);
        if !path.exists() || !path.is_file() {
            return Err(QueryError::DataSourceError { message: "File doesn't exist".into() });
        }
        if path.extension().map(|e| e.to_ascii_lowercase()) != Some(OsString::from("csv")) {
            return Err(QueryError::DataSourceError { message: "Not a CSV file".into() });
        }
        Ok(Self(path.to_path_buf()))
    }
}

#[derive(Debug)]
pub struct CsvDataSource {
    file_path: ValidCsvPath,
    original_schema: Vec<Field>,
}

impl CsvDataSource {
    pub fn new(file_path: String) -> Result<Self, QueryError> {

        let file_path = ValidCsvPath::new(&file_path)?;


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
    pub fn infer_schema(file_path: &ValidCsvPath) -> Result<Vec<Field>, String> {
        if let Ok(file) = File::open(&file_path.0) {
            let mut buf_file = BufReader::new(file);
            println!("This is the buf_file {:?}", buf_file);

            let mut lines = Vec::new();
            let mut line = String::new();
            for _ in 0..101 {
                line.clear();
                if let Ok(val) = buf_file.read_line(&mut line) {
                    if val == 0 {
                        break;
                    } else {
                        lines.push(line.clone());
                    }
                } else {
                    return Err("Failed to read file buffer".to_string())
                }
            }

            let first_line = lines.get(0);
            match first_line {
                Some(line) => {
                    println!("This is the first line: {:?}", line);
                    if let Some(_second_line) = lines.get(1) {
                        let types = Self::detect_types(&lines, line.trim().split(",").collect::<Vec<&str>>().len());
                        Ok(line.trim().split(",").enumerate().map(move |(i, header)| {
                            Field {
                                name: header.trim().to_string(),
                                field_type: types[i],
                                is_nullable: true
                            }
                        }).collect::<Vec<Field>>())
                    } else {
                        Ok(line.trim().split(",").map(move |header| {
                            Field {
                                name: header.trim().to_string(),
                                field_type: DataType::String,
                                is_nullable: true
                            }
                        }).collect::<Vec<Field>>())
                        
                    }
                },
                None => {
                    Err("Failed to infer schema".to_string())
                }
            }

        } else {
            Err("Failed to open file".to_string())
        }


    }

    fn detect_types(lines: &Vec<String>, columns: usize) -> Vec<DataType> {
        let mut types: Vec<DataType> = Vec::with_capacity(columns);
        let mut columnar_data: Vec<Vec<&str>> = Vec::with_capacity(columns);
        for _ in 0..columns {
            types.push(DataType::String);
            columnar_data.push(vec![]);
        }
        for line in lines.iter().skip(1).take(100) {
            let line = line.trim().split(",").collect::<Vec<&str>>();
            for i in 0..columns {
                if let Some(item) = line.get(i) {
                    columnar_data[i].push(item.trim());
                } else {
                    columnar_data[i].push("");
                }
            };
        };
        println!("This is the columnar_data: {:?}", columnar_data);
        for (index, data) in columnar_data.iter().enumerate() {

            let is_all_empty = data.iter().all(|data| {
                data.is_empty()
            });

            let is_float = data.iter().all(|data| {
                if data.is_empty() { 
                    return true;
                };
                let number = data.parse::<f64>();
                match number {
                    Ok(_number) => true,
                    Err(_error) => false,
                }
            });
            let is_int = data.iter().all(|data| {
                if data.is_empty() { 
                    return true;
                };
                let number = data.parse::<i32>();
                match number {
                    Ok(_number) => true,
                    Err(_error) => false,
                }
            });

            let is_boolean = data.iter().all(|data| {
                if data.is_empty() {
                    return true;
                };
                let boolean = data.to_lowercase().parse::<bool>();
                match boolean {
                    Ok(_boolean) => true,
                    Err(_error) => false,
                }
            });
                
            if !is_all_empty {
                if is_int {
                    types[index] = DataType::Int32;
                } else if is_float {
                    types[index] = DataType::Float64;
                } else if is_boolean {
                    types[index] = DataType::Bool;
                }
            }

        };
        types
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