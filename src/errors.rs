#[derive(Debug)]
pub enum QueryError {
    SchemaCountMismatch {
        expected: usize,
        actual: usize
    },
    ColumnLengthMismatch {
        column_index: usize,
        expected_length: usize,
        actual_length: usize,
    },
    TypeMismatch {
        column_name: String,
        expected: String,
        actual: String,
    },
    DataSourceError {
        message: String,
    },
    ValidationError {
        message: String,
    }
}

pub enum LexerError {
    InvalidToken {
        message: String,
    }
}