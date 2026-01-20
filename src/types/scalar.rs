#[derive(Debug)]
pub enum ScalarValue {
    Int32(Option<i32>),
    String(Option<String>),
    Bool(Option<bool>),
    Float64(Option<f64>)
}

#[derive(Debug)]
pub struct ColumnVector {
    pub values: Vec<ScalarValue>,
}

impl ColumnVector {
    pub fn new(values: Vec<ScalarValue>) -> Self {
        Self {
            values
        }
    }
}
