use std::{collections::HashMap, fmt::Binary};

use crate::{DataSource, DataType, ScalarValue, Schema, errors::QueryError};


#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        path: String,
        schema: Schema,
        projection: Vec<String>
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression
    },
    Projection {
        input: Box<LogicalPlan>,
        columns: Expression
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum UnaryOperator {
    Not,
    IsNull,
    IsNotNull,
    Negate,
}


/// This is the enum for expressions
#[derive(Debug)]
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
    fn is_valid(&self) -> Result<(), String> {
        todo!();
    }
}

pub struct Catalog {
    tables: HashMap<String, Box<dyn DataSource>>,
}

impl Catalog {
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

pub struct PlanBuilder {
    catalog: Catalog,
    current_plan: Option<LogicalPlan>,
    current_schema: Schema,
}

impl PlanBuilder {
    pub fn scan(self, table_name: &str) -> Result<PlanBuilder, QueryError> {
        let source = self.catalog.tables.get(table_name);
        match source {
            Some(source) => {
                let schema = source.schema();
                let projection = schema.fields.iter().map(|f| {
                    f.name.clone()
                }).collect::<Vec<String>>();
                let current_plan = LogicalPlan::Scan { path: table_name.to_string() , schema: schema.clone(), projection: projection };
                Ok(PlanBuilder {  current_schema: schema.clone(), current_plan: Some(current_plan), ..self  })
            },
            None => Err(QueryError::ValidationError { message: "Table not found".to_string() })
        }
    }

    pub fn filter(self, expression: Expression) -> Result<PlanBuilder, QueryError> {

        let input = self.current_plan.ok_or_else(|| {
            QueryError::ValidationError { message: "Filter did not receive current plan".to_string() }
        })?;

        expression.is_valid().map_err(|err| {
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
}