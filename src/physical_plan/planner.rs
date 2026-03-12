use crate::errors::QueryError;
use crate::{DataType, Field, Schema};
use crate::logical_plan::plan::{Catalog, Expression, LogicalPlan};

use super::eval::{BinaryExpr, ColumnExpr, LiteralExpr, PhysicalExpr, UnaryExpr};
use super::plan::{FilterExec, PhysicalPlan, ProjectionExec, ScanExec};

/// Converts a LogicalPlan tree into an executable PhysicalPlan tree.
pub fn create_physical_plan(
    plan: &LogicalPlan,
    catalog: &Catalog,
) -> Result<Box<dyn PhysicalPlan>, QueryError> {
    match plan {
        LogicalPlan::Scan { path, schema, .. } => {
            let source = catalog.get_source(path)?;
            Ok(Box::new(ScanExec {
                source,
                schema: schema.clone(),
            }))
        }
        LogicalPlan::Filter { input, predicate } => {
            let physical_input = create_physical_plan(input, catalog)?;
            let input_schema = physical_input.schema().clone();
            let physical_predicate = create_physical_expr(predicate, &input_schema)?;
            Ok(Box::new(FilterExec {
                input: physical_input,
                predicate: physical_predicate,
            }))
        }
        LogicalPlan::Projection { input, columns } => {
            let physical_input = create_physical_plan(input, catalog)?;
            let input_schema = physical_input.schema().clone();

            let physical_exprs: Result<Vec<_>, _> = columns
                .iter()
                .map(|expr| create_physical_expr(expr, &input_schema))
                .collect();

            let output_fields: Vec<Field> = columns
                .iter()
                .map(|expr| match expr {
                    Expression::Column { name, data_type } => Field {
                        name: name.clone(),
                        field_type: *data_type,
                        is_nullable: true,
                    },
                    Expression::Literal(scalar) => {
                        let dt = match scalar {
                            crate::ScalarValue::Int32(_) => DataType::Int32,
                            crate::ScalarValue::Float64(_) => DataType::Float64,
                            crate::ScalarValue::Bool(_) => DataType::Bool,
                            crate::ScalarValue::String(_) => DataType::String,
                        };
                        Field {
                            name: format!("{:?}", scalar),
                            field_type: dt,
                            is_nullable: true,
                        }
                    }
                    _ => Field {
                        name: format!("{:?}", expr),
                        field_type: DataType::Int32,
                        is_nullable: true,
                    },
                })
                .collect();

            Ok(Box::new(ProjectionExec {
                input: physical_input,
                schema: Schema::new(output_fields),
                exprs: physical_exprs?,
            }))
        }
    }
}

/// Converts a logical Expression into a physical PhysicalExpr.
/// Column names are resolved to indexes here so the executor never searches by name.
fn create_physical_expr(
    expr: &Expression,
    schema: &Schema,
) -> Result<Box<dyn PhysicalExpr>, QueryError> {
    match expr {
        Expression::Column { name, .. } => {
            let index = schema
                .fields
                .iter()
                .position(|f| f.name == *name)
                .ok_or_else(|| QueryError::ValidationError {
                    message: format!("Column '{}' not found in schema", name),
                })?;
            Ok(Box::new(ColumnExpr { index }))
        }
        Expression::Literal(scalar) => Ok(Box::new(LiteralExpr {
            value: scalar.clone(),
        })),
        Expression::Binary {
            left,
            right,
            operator,
        } => {
            let l = create_physical_expr(left, schema)?;
            let r = create_physical_expr(right, schema)?;
            Ok(Box::new(BinaryExpr {
                left: l,
                right: r,
                op: operator.clone(),
            }))
        }
        Expression::Unary { operand, operator } => {
            let o = create_physical_expr(operand, schema)?;
            Ok(Box::new(UnaryExpr {
                operand: o,
                op: operator.clone(),
            }))
        }
    }
}
