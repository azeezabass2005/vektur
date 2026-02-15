use crate::errors::LexerError;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse a SQL query string into an AST using sqlparser
/// 
/// Returns the parsed SQL statement(s) as sqlparser's AST.
/// You can then convert this AST to your LogicalPlan.
pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>, LexerError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql)
        .map_err(|e| LexerError::InvalidToken {
            message: format!("SQL parsing error: {}", e),
        })
}

/// Re-export sqlparser AST types for convenience
pub use sqlparser::ast::{Statement, Query, Select, Expr, BinaryOperator, UnaryOperator};

/// Re-export planner for converting AST to LogicalPlan
pub use crate::sql_support::planner::sql_to_logical_plan;