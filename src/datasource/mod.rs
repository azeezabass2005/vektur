use crate::{Field, RecordBatch, errors::QueryError};

pub trait DataSource {
    /// The schema method that returns the schema of every data source
    fn schema(&self) -> &Vec<Field>;

    /// The scan method that returns the record batch if the data source is scanned successfully or a QueryError
    fn scan(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>>;
}

pub mod csv;