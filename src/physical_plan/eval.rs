use crate::errors::QueryError;
use crate::logical_plan::plan::{Operator, UnaryOperator};
use crate::{ColumnVector, RecordBatch, ScalarValue};

pub trait PhysicalExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError>;
}

/// Retrieves a column from the batch by its position index.
pub struct ColumnExpr {
    pub index: usize,
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        Ok(batch.columns[self.index].clone())
    }
}

/// Produces a column where every row has the same literal value.
pub struct LiteralExpr {
    pub value: ScalarValue,
}

impl PhysicalExpr for LiteralExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        let row_count = if batch.columns.is_empty() {
            0
        } else {
            batch.columns[0].values.len()
        };
        let values = vec![self.value.clone(); row_count];
        Ok(ColumnVector::new(values))
    }
}

/// Evaluates two child expressions and combines them element-wise with an operator.
pub struct BinaryExpr {
    pub left: Box<dyn PhysicalExpr>,
    pub right: Box<dyn PhysicalExpr>,
    pub op: Operator,
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        let left_col = self.left.evaluate(batch)?;
        let right_col = self.right.evaluate(batch)?;

        let values = left_col
            .values
            .iter()
            .zip(right_col.values.iter())
            .map(|(l, r)| apply_operator(l, r, &self.op))
            .collect::<Result<Vec<ScalarValue>, QueryError>>()?;

        Ok(ColumnVector::new(values))
    }
}

/// Evaluates one child expression and applies a unary operator element-wise.
pub struct UnaryExpr {
    pub operand: Box<dyn PhysicalExpr>,
    pub op: UnaryOperator,
}

impl PhysicalExpr for UnaryExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        let col = self.operand.evaluate(batch)?;
        let values = col.values.iter().map(|v| apply_unary(&self.op, v)).collect();
        Ok(ColumnVector::new(values))
    }
}

fn is_null(v: &ScalarValue) -> bool {
    matches!(
        v,
        ScalarValue::Int32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::String(None)
            | ScalarValue::Bool(None)
    )
}

fn apply_unary(op: &UnaryOperator, value: &ScalarValue) -> ScalarValue {
    match (op, value) {
        (UnaryOperator::Not, ScalarValue::Bool(b)) => ScalarValue::Bool(b.map(|x| !x)),
        (UnaryOperator::Negate, ScalarValue::Int32(n)) => ScalarValue::Int32(n.map(|x| -x)),
        (UnaryOperator::Negate, ScalarValue::Float64(n)) => ScalarValue::Float64(n.map(|x| -x)),
        (UnaryOperator::IsNull, v) => ScalarValue::Bool(Some(is_null(v))),
        (UnaryOperator::IsNotNull, v) => ScalarValue::Bool(Some(!is_null(v))),
        _ => value.clone(),
    }
}

fn apply_operator(
    left: &ScalarValue,
    right: &ScalarValue,
    op: &Operator,
) -> Result<ScalarValue, QueryError> {
    match (left, right) {
        (ScalarValue::Int32(l), ScalarValue::Int32(r)) => match (l, r) {
            (Some(l), Some(r)) => apply_int32(*l, *r, op),
            _ => Ok(null_for_op(op, true)),
        },
        (ScalarValue::Float64(l), ScalarValue::Float64(r)) => match (l, r) {
            (Some(l), Some(r)) => apply_float64(*l, *r, op),
            _ => Ok(null_for_op(op, false)),
        },
        (ScalarValue::Int32(l), ScalarValue::Float64(r)) => match (l, r) {
            (Some(l), Some(r)) => apply_float64(*l as f64, *r, op),
            _ => Ok(null_for_op(op, false)),
        },
        (ScalarValue::Float64(l), ScalarValue::Int32(r)) => match (l, r) {
            (Some(l), Some(r)) => apply_float64(*l, *r as f64, op),
            _ => Ok(null_for_op(op, false)),
        },
        (ScalarValue::String(l), ScalarValue::String(r)) => match (l, r) {
            (Some(l), Some(r)) => apply_string(l, r, op),
            _ => Ok(ScalarValue::Bool(None)),
        },
        (ScalarValue::Bool(l), ScalarValue::Bool(r)) => match (l, r) {
            (Some(l), Some(r)) => apply_bool(*l, *r, op),
            _ => Ok(ScalarValue::Bool(None)),
        },
        _ => Err(QueryError::TypeMismatch {
            column_name: String::new(),
            expected: format!("{:?}", left),
            actual: format!("{:?}", right),
        }),
    }
}

/// Returns the appropriate null value depending on whether the operator
/// produces a boolean (comparison) or numeric (arithmetic) result.
fn null_for_op(op: &Operator, is_int: bool) -> ScalarValue {
    match op {
        Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
            if is_int {
                ScalarValue::Int32(None)
            } else {
                ScalarValue::Float64(None)
            }
        }
        _ => ScalarValue::Bool(None),
    }
}

fn apply_int32(l: i32, r: i32, op: &Operator) -> Result<ScalarValue, QueryError> {
    Ok(match op {
        Operator::Add => ScalarValue::Int32(Some(l + r)),
        Operator::Subtract => ScalarValue::Int32(Some(l - r)),
        Operator::Multiply => ScalarValue::Int32(Some(l * r)),
        Operator::Divide => {
            if r == 0 {
                ScalarValue::Int32(None)
            } else {
                ScalarValue::Int32(Some(l / r))
            }
        }
        Operator::Eq => ScalarValue::Bool(Some(l == r)),
        Operator::NotEq => ScalarValue::Bool(Some(l != r)),
        Operator::Gt => ScalarValue::Bool(Some(l > r)),
        Operator::Lt => ScalarValue::Bool(Some(l < r)),
        Operator::GtEq => ScalarValue::Bool(Some(l >= r)),
        Operator::LtEq => ScalarValue::Bool(Some(l <= r)),
    })
}

fn apply_float64(l: f64, r: f64, op: &Operator) -> Result<ScalarValue, QueryError> {
    Ok(match op {
        Operator::Add => ScalarValue::Float64(Some(l + r)),
        Operator::Subtract => ScalarValue::Float64(Some(l - r)),
        Operator::Multiply => ScalarValue::Float64(Some(l * r)),
        Operator::Divide => {
            if r == 0.0 {
                ScalarValue::Float64(None)
            } else {
                ScalarValue::Float64(Some(l / r))
            }
        }
        Operator::Eq => ScalarValue::Bool(Some(l == r)),
        Operator::NotEq => ScalarValue::Bool(Some(l != r)),
        Operator::Gt => ScalarValue::Bool(Some(l > r)),
        Operator::Lt => ScalarValue::Bool(Some(l < r)),
        Operator::GtEq => ScalarValue::Bool(Some(l >= r)),
        Operator::LtEq => ScalarValue::Bool(Some(l <= r)),
    })
}

fn apply_string(l: &str, r: &str, op: &Operator) -> Result<ScalarValue, QueryError> {
    match op {
        Operator::Eq => Ok(ScalarValue::Bool(Some(l == r))),
        Operator::NotEq => Ok(ScalarValue::Bool(Some(l != r))),
        Operator::Gt => Ok(ScalarValue::Bool(Some(l > r))),
        Operator::Lt => Ok(ScalarValue::Bool(Some(l < r))),
        Operator::GtEq => Ok(ScalarValue::Bool(Some(l >= r))),
        Operator::LtEq => Ok(ScalarValue::Bool(Some(l <= r))),
        _ => Err(QueryError::ValidationError {
            message: format!("Operator {:?} not supported for strings", op),
        }),
    }
}

fn apply_bool(l: bool, r: bool, op: &Operator) -> Result<ScalarValue, QueryError> {
    match op {
        Operator::Eq => Ok(ScalarValue::Bool(Some(l == r))),
        Operator::NotEq => Ok(ScalarValue::Bool(Some(l != r))),
        _ => Err(QueryError::ValidationError {
            message: format!("Operator {:?} not supported for booleans", op),
        }),
    }
}
