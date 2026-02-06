use std::{collections::HashMap, fmt::{Display, Formatter}};

use crate::{DataSource, DataType, Field, ScalarValue, Schema, datasource::csv::{CsvDataSource, ValidCsvPath}, errors::QueryError, logical_plan};


#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        path: String,
        schema: Schema,
        projection: Option<Vec<String>>
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression
    },
    Projection {
        input: Box<LogicalPlan>,
        columns: Vec<Expression>
    }
}


impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut Formatter<'_>, indent: usize) -> std::fmt::Result {
        let indent_str = "  ".repeat(indent);
        match self {
            LogicalPlan::Scan { path, projection, .. } => {
                write!(f, "{}Scan: {} (columns: {:?})", indent_str, path, projection)
            },
            LogicalPlan::Filter { input, predicate } => {
                write!(f, "{}Filter: {:?}\n", indent_str, predicate)?;
                input.fmt_with_indent(f, indent + 1)
            },
            LogicalPlan::Projection { input, columns } => {
                let col_names: Vec<String> = columns.iter().map(|c| {
                    if let Expression::Column { name, .. } = c {
                        name.clone()
                    } else {
                        format!("{:?}", c)
                    }
                }).collect();
                write!(f, "{}Projection: {:?}\n", indent_str, col_names)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Operator {
    Gt,
    Lt,
    Eq,
    NotEq,
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Not,
    IsNull,
    IsNotNull,
    Negate,
}

 
#[derive(Debug, Clone)]
pub enum Expression {
    Column {
        name: String,
        data_type: DataType
    },
    Literal(ScalarValue),
    Binary {
        left: Box<Expression>,
        right: Box<Expression>,
        operator: Operator,
    },
    Unary {
        operand: Box<Expression>,
        operator: UnaryOperator
    }
}

impl Expression {

    fn get_data_type(&self, schema: &Schema) -> Result<DataType, String> {
        match self {
            Expression::Column { data_type, .. } => Ok(*data_type),
            Expression::Literal(scalar) => {
                match scalar {
                    ScalarValue::Int32(_) => Ok(DataType::Int32),
                    ScalarValue::String(_) => Ok(DataType::String),
                    ScalarValue::Bool(_) => Ok(DataType::Bool),
                    ScalarValue::Float64(_) => Ok(DataType::Float64),
                }
            },
            Expression::Binary { left, right, operator } => {
                let left_type = left.get_data_type(schema)?;
                let right_type = right.get_data_type(schema)?;
                
                match operator {
                    Operator::Gt | Operator::Lt | Operator::Eq | Operator::NotEq => {
                        if Self::are_compatible_for_comparison(&left_type, &right_type) {
                            Ok(DataType::Bool)
                        } else {
                            Err(format!(
                                "Incompatible types for comparison: {:?} and {:?}",
                                left_type, right_type
                            ))
                        }
                    },
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                        if Self::are_compatible_for_arithmetic(&left_type, &right_type) {
                            if left_type == DataType::Float64 || right_type == DataType::Float64 {
                                Ok(DataType::Float64)
                            } else {
                                Ok(DataType::Int32)
                            }
                        } else {
                            Err(format!(
                                "Incompatible types for arithmetic: {:?} and {:?}",
                                left_type, right_type
                            ))
                        }
                    },
                }
            },
            Expression::Unary { operand, operator } => {
                let operand_type = operand.get_data_type(schema)?;
                match operator {
                    UnaryOperator::Not => {
                        if operand_type == DataType::Bool {
                            Ok(DataType::Bool)
                        } else {
                            Err(format!("Not operator requires Bool, got {:?}", operand_type))
                        }
                    },
                    UnaryOperator::IsNull | UnaryOperator::IsNotNull => Ok(DataType::Bool),
                    UnaryOperator::Negate => {
                        match operand_type {
                            DataType::Int32 | DataType::Float64 => Ok(operand_type),
                            _ => Err(format!("Negate operator requires numeric type, got {:?}", operand_type))
                        }
                    },
                }
            },
        }
    }


    fn are_compatible_for_comparison(left: &DataType, right: &DataType) -> bool {
        match (left, right) {
            (DataType::Int32, DataType::Int32) => true,
            (DataType::Float64, DataType::Float64) => true,
            (DataType::Int32, DataType::Float64) => true,
            (DataType::Float64, DataType::Int32) => true,
            (DataType::String, DataType::String) => true,
            (DataType::Bool, DataType::Bool) => true,
            _ => false,
        }
    }

    fn are_compatible_for_arithmetic(left: &DataType, right: &DataType) -> bool {
        match (left, right) {
            (DataType::Int32, DataType::Int32) => true,
            (DataType::Float64, DataType::Float64) => true,
            (DataType::Int32, DataType::Float64) => true,
            (DataType::Float64, DataType::Int32) => true,
            _ => false,
        }
    }

    fn is_valid(&self, schema: &Schema) -> Result<(), String> {
        match self {
            Expression::Column { name, data_type } => {
                let field = schema.column_exists(name)?;
                if field.field_type != *data_type {
                    return Err(format!(
                        "Column {} type mismatch: expected {:?} got {:?}",
                        name, field.field_type, data_type
                    ))
                };
                Ok(())
            },
            Expression::Binary { left, right, operator } => {
                left.is_valid(&schema)?;
                right.is_valid(&schema)?;                
                let left_type = left.get_data_type(schema)?;
                let right_type = right.get_data_type(schema)?;
                
                match operator {
                    Operator::Gt | Operator::Lt | Operator::Eq | Operator::NotEq => {
                        if !Self::are_compatible_for_comparison(&left_type, &right_type) {
                            return Err(format!(
                                "Incompatible types for {} operator: {:?} and {:?}",
                                format!("{:?}", operator), left_type, right_type
                            ));
                        }
                    },
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                        if !Self::are_compatible_for_arithmetic(&left_type, &right_type) {
                            return Err(format!(
                                "Incompatible types for {} operator: {:?} and {:?}",
                                format!("{:?}", operator), left_type, right_type
                            ));
                        }
                    },
                }
                
                Ok(())
            },
            Expression::Unary { operand, operator } => {
                operand.is_valid(schema)?;
                Ok(())
            },
            _ => Ok(())
        }
    }
}

pub struct Catalog {
    tables: HashMap<String, Box<dyn DataSource>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new()
        }
    }

    pub fn register_table(&mut self, name: String,source: Box<dyn DataSource>, ) {
        self.tables.insert(name, source);
    }

    pub fn get_schema(&self, table_name: &str) -> Option<&Schema> {
        let table = self.tables.get(table_name);
        match table {
            Some(source) => {
                Some(source.schema())
            },
            None => None
        }
    }
}


pub struct ExecutionContext {
    catalog: Catalog
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            catalog: Catalog {
                tables: HashMap::new()
            }
        }
    }

    pub fn csv(&self, file_path: String) -> Result<DataFrame, QueryError> {

        let data_source = CsvDataSource::new(file_path.clone())?;
 

        let schema = data_source.schema();

        let plan = LogicalPlan::Scan {
            path: file_path, schema: schema.clone(), projection: None,
        };
        Ok(DataFrame { plan })
    }
}

pub struct DataFrame {
    plan: LogicalPlan
}

impl DataFrame {
    pub fn project(self, columns: Vec<Expression>) -> Self {
        Self {
            plan: LogicalPlan::Projection { input: Box::new(self.plan), columns: columns }
        }
    }

    pub fn filter(self, predicate: Expression) -> Self {
        Self {
            plan: LogicalPlan::Filter { input: Box::new(self.plan), predicate }
        }
    }

    pub fn schema(&self) -> Schema {
        match &self.plan {
            LogicalPlan::Scan { schema, .. } => {
                schema.clone()
            },
            LogicalPlan::Filter { input, .. } => {
                DataFrame { plan: *input.clone() }.schema()
            },
            LogicalPlan::Projection { input, columns } => {
                let input_schema = DataFrame { plan: *input.clone() }.schema();
                
                let new_fields: Vec<Field> = columns.iter().map(|expr| {
                    let data_type = expr.get_data_type(&input_schema).unwrap();

                    match expr {
                        Expression::Column { name, .. } => {
                            Field {
                                name: name.clone(),
                                field_type: data_type,
                                is_nullable: true,
                            }
                        },
                        _ => {
                            Field {
                                name: format!("{:?}", expr),
                                field_type: data_type,
                                is_nullable: true,
                            }
                        }
                    }
                }).collect();
                
                Schema::new(new_fields)
            }
        }
    }

    pub fn plan(&self) -> LogicalPlan {
        self.plan.clone()
    }
}

pub struct PlanBuilder<'a> {
    catalog: &'a Catalog,
    current_plan: Option<LogicalPlan>,
    current_schema: Schema,
}

impl<'a> PlanBuilder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog: catalog,
            current_plan: None,
            current_schema: Schema::new(Vec::new())
        }
    }

    pub fn scan(self, table_name: &str) -> Result<PlanBuilder<'a>, QueryError> {
        let source = self.catalog.tables.get(table_name);
        match source {
            Some(source) => {
                let schema = source.schema();
                let projection = schema.fields.iter().map(|f| {
                    f.name.clone()
                }).collect::<Vec<String>>();
                let current_plan = LogicalPlan::Scan { path: table_name.to_string() , schema: schema.clone(), projection: Some(projection) };
                Ok(PlanBuilder {  current_schema: schema.clone(), current_plan: Some(current_plan), ..self  })
            },
            None => Err(QueryError::ValidationError { message: "Table not found".to_string() })
        }
    }

    pub fn filter(self, expression: Expression) -> Result<PlanBuilder<'a>, QueryError> {

        let input = self.current_plan.ok_or_else(|| {
            QueryError::ValidationError { message: "Filter did not receive current plan".to_string() }
        })?;
        expression.is_valid(&self.current_schema).map_err(|err| {
            return QueryError::ValidationError { message: err };
        })?;
        if !matches!(expression, Expression::Binary { .. } | Expression::Unary { .. }) {
            return Err(QueryError::ValidationError { message: "Expression Type not supported for filtering".to_string() })
        }
        let plan = LogicalPlan::Filter { input: Box::new(input), predicate: expression  };
        Ok(PlanBuilder{
            current_plan: Some(plan),
            ..self
        })
    }

    pub fn project(self, columns: Vec<String>) -> Result<PlanBuilder<'a>, QueryError> {
        let input = self.current_plan.ok_or_else(|| {
            QueryError::ValidationError { message: "Projection did not receive current plan".to_string() }
        })?;
        
        let columns_expr = columns.into_iter().map(|col| {
            let field = self.current_schema.column_exists(&col).map_err(|err| 
                QueryError::ValidationError { message: err })?;
            Ok(Expression::Column { name: field.name.clone(), data_type: field.field_type })
        }).collect::<Result<Vec<Expression>, QueryError>>()?;

        let plan = LogicalPlan::Projection { input: Box::new(input), columns: columns_expr.clone() };
        let new_schema_fields: Vec<Field> = columns_expr.iter().map(|expr| {
            if let Expression::Column { name, data_type } = expr {
                Field {
                    name: name.clone(),
                    field_type: *data_type,
                    is_nullable: true,
                }
            } else {
                todo!("Non-column expressions in projection")
            }
        }).collect();
        
        let new_schema = Schema::new(new_schema_fields);

        Ok(PlanBuilder{
            current_plan: Some(plan),
            current_schema: new_schema,
            ..self
        })
    }

    pub fn build(self) -> Result<LogicalPlan, QueryError> {
        let current_plan = self.current_plan.ok_or_else(|| {
            return QueryError::DataSourceError { message: format!("Failed to build logical plan") }
        })?;
        Ok(current_plan)
    }
}