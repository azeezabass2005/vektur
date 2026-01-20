use crate::{ColumnVector, DataType, errors::QueryError};

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: DataType,
    pub is_nullable: bool,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

#[derive(Debug)]
pub struct RecordBatch {
    pub schema: Schema,
    pub columns: Vec<ColumnVector>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self {
            fields
        }
    }
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<ColumnVector>) -> Result<Self, QueryError> {

        if schema.fields.len() != columns.len() {
            return Err(QueryError::SchemaCountMismatch {
                expected: schema.fields.len(),
                actual: columns.len(),
            });
        }

        if !columns.is_empty() {
            let height = columns[0].values.len();
            for (index, column) in columns.iter().enumerate() {
                if column.values.len() != height {
                    return Err(QueryError::ColumnLengthMismatch {
                        column_index: index,
                        expected_length: height,
                        actual_length: column.values.len()
                    });
                }
            }
        }

        Ok(Self {
            schema,
            columns
        })
    }
}