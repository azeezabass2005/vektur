use sqlparser::ast::{Statement, SetExpr, SelectItem, Expr, BinaryOperator as SqlBinaryOp, Value};
use crate::logical_plan::plan::{LogicalPlan, Expression, Operator, Catalog};
use crate::{ScalarValue, Schema, errors::QueryError};

/// Convert a sqlparser AST Statement into a LogicalPlan
pub fn sql_to_logical_plan(
    statement: &Statement,
    catalog: &Catalog,
) -> Result<LogicalPlan, QueryError> {
    match statement {
        Statement::Query(query) => {
            query_to_logical_plan(query, catalog)
        },
        _ => Err(QueryError::ValidationError {
            message: format!("Unsupported statement type: {:?}", statement),
        }),
    }
}

fn query_to_logical_plan(
    query: &sqlparser::ast::Query,
    catalog: &Catalog,
) -> Result<LogicalPlan, QueryError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return Err(QueryError::ValidationError {
            message: "Only SELECT queries are supported".to_string(),
        }),
    };

    let table_name = extract_table_name(select)?;
    
    let schema = catalog.get_schema(&table_name)
        .ok_or_else(|| QueryError::ValidationError {
            message: format!("Table '{}' not found in catalog", table_name),
        })?
        .clone();

    let mut plan = LogicalPlan::Scan {
        path: table_name.clone(),
        schema: schema.clone(),
        projection: None,
    };

    if let Some(selection) = &select.selection {
        let predicate = sql_expr_to_expression(selection, &schema)?;
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    let projection_columns: Vec<Expression> = select
        .projection
        .iter()
        .map(|item| select_item_to_expression(item, &schema))
        .collect::<Result<Vec<_>, _>>()?;

    if !projection_columns.is_empty() {
        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            columns: projection_columns,
        };
    }

    Ok(plan)
}

/// Extract table name from FROM clause
fn extract_table_name(select: &sqlparser::ast::Select) -> Result<String, QueryError> {
    if select.from.is_empty() {
        return Err(QueryError::ValidationError {
            message: "Query must have a FROM clause".to_string(),
        });
    }

    let table_with_joins = &select.from[0];
    match &table_with_joins.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            if name.0.is_empty() {
                return Err(QueryError::ValidationError {
                    message: "Invalid table name".to_string(),
                });
            }

            let table_name_str = name.to_string();
            Ok(table_name_str.trim_matches('"').trim_matches('\'').to_string())
        }
        _ => Err(QueryError::ValidationError {
            message: "Only simple table references are supported (no subqueries, joins, etc.)".to_string(),
        }),
    }
}

/// Convert a SELECT item (column) to an Expression
fn select_item_to_expression(
    item: &SelectItem,
    schema: &Schema,
) -> Result<Expression, QueryError> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            sql_expr_to_expression(expr, schema)
        }
        SelectItem::ExprWithAlias { expr, .. } => {
            sql_expr_to_expression(expr, schema)
        }
        SelectItem::Wildcard(_) => {
            Err(QueryError::ValidationError {
                message: "SELECT * is not yet supported in projection".to_string(),
            })
        }
        _ => Err(QueryError::ValidationError {
            message: format!("Unsupported SELECT item: {:?}", item),
        }),
    }
}

/// Convert sqlparser Expr to your Expression enum
fn sql_expr_to_expression(
    expr: &Expr,
    schema: &Schema,
) -> Result<Expression, QueryError> {
    match expr {
        Expr::Identifier(ident) => {
            let name = ident.value.clone();
            let field = schema.column_exists(&name)
                .map_err(|e| QueryError::ValidationError { message: e })?;
            Ok(Expression::Column {
                name,
                data_type: field.field_type,
            })
        }
        Expr::Value(value_with_span) => {
            let scalar = sql_value_to_scalar(&value_with_span.value)?;
            Ok(Expression::Literal(scalar))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_expr = sql_expr_to_expression(left, schema)?;
            let right_expr = sql_expr_to_expression(right, schema)?;
            let operator = sql_binary_op_to_operator(op)?;
            Ok(Expression::Binary {
                left: Box::new(left_expr),
                right: Box::new(right_expr),
                operator,
            })
        }
        Expr::UnaryOp { op, expr } => {
            let operand = sql_expr_to_expression(expr, schema)?;
            let operator = sql_unary_op_to_operator(op)?;
            Ok(Expression::Unary {
                operand: Box::new(operand),
                operator,
            })
        }
        _ => Err(QueryError::ValidationError {
            message: format!("Unsupported expression type: {:?}", expr),
        }),
    }
}

/// Convert sqlparser BinaryOperator to your Operator enum
fn sql_binary_op_to_operator(op: &SqlBinaryOp) -> Result<Operator, QueryError> {
    match op {
        SqlBinaryOp::Eq => Ok(Operator::Eq),
        SqlBinaryOp::NotEq => Ok(Operator::NotEq),
        SqlBinaryOp::Gt => Ok(Operator::Gt),
        SqlBinaryOp::Lt => Ok(Operator::Lt),
        SqlBinaryOp::GtEq => Ok(Operator::Gt), // TODO: I will add greater than or equal to operator
        SqlBinaryOp::LtEq => Ok(Operator::Lt), // TODO: Same for less than or equal to
        SqlBinaryOp::Plus => Ok(Operator::Add),
        SqlBinaryOp::Minus => Ok(Operator::Subtract),
        SqlBinaryOp::Multiply => Ok(Operator::Multiply),
        SqlBinaryOp::Divide => Ok(Operator::Divide),
        _ => Err(QueryError::ValidationError {
            message: format!("Unsupported binary operator: {:?}", op),
        }),
    }
}

/// Convert sqlparser UnaryOperator to your UnaryOperator enum
fn sql_unary_op_to_operator(
    op: &sqlparser::ast::UnaryOperator,
) -> Result<crate::logical_plan::plan::UnaryOperator, QueryError> {
    match op {
        sqlparser::ast::UnaryOperator::Not => Ok(crate::logical_plan::plan::UnaryOperator::Not),
        sqlparser::ast::UnaryOperator::Minus => Ok(crate::logical_plan::plan::UnaryOperator::Negate),
        sqlparser::ast::UnaryOperator::Plus => Ok(crate::logical_plan::plan::UnaryOperator::Negate),
        _ => Err(QueryError::ValidationError {
            message: format!("Unsupported unary operator: {:?}", op),
        }),
    }
}

/// Convert sqlparser Value to your ScalarValue
fn sql_value_to_scalar(value: &Value) -> Result<ScalarValue, QueryError> {
    match value {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i32>() {
                Ok(ScalarValue::Int32(Some(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                Err(QueryError::ValidationError {
                    message: format!("Invalid number: {}", n),
                })
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(ScalarValue::String(Some(s.clone())))
        }
        Value::Boolean(b) => Ok(ScalarValue::Bool(Some(*b))),
        Value::Null => Ok(ScalarValue::String(None)), // TODO: Handle nulls properly
        _ => Err(QueryError::ValidationError {
            message: format!("Unsupported value type: {:?}", value),
        }),
    }
}
