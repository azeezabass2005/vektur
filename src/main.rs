use vektur::{
    DataType, ScalarValue, datasource::csv::CsvDataSource, errors::LexerError, logical_plan::plan::{Catalog, Expression, Operator, PlanBuilder}, sql_support::sql::{parse_sql, sql_to_logical_plan}
};

fn main() {
    let csv_path = "test/students.csv";

    let sql_query = "SELECT Name, Email FROM students WHERE IsVerified = true";
    
    let mut catalog = Catalog::new();
    match CsvDataSource::new(csv_path.to_string()) {
        Ok(csv_source) => {
            catalog.register_table("students".to_string(), Box::new(csv_source));
            println!("✓ Registered 'students' table from {}\n", csv_path);
        },
        Err(err) => {
            println!("Error loading CSV file: {:?}", err);
            return;
        }
    }
    
    println!("=== SQL to Logical Plan Conversion ===\n");
    println!("SQL Query: {}\n", sql_query);
    
    match parse_sql(sql_query) {
        Ok(statements) => {
            if let Some(statement) = statements.first() {
                match sql_to_logical_plan(statement, &catalog) {
                    Ok(plan) => {
                        println!("Generated Logical Plan:\n{}\n", plan);
                    },
                    Err(e) => {
                        println!("Error converting to logical plan: {:?}\n", e);
                    }
                }
            }
        },
        Err(e) => {
            match e {
                LexerError::InvalidToken { message } => println!("Parse error: {}", message),
            }
        }
    }
    
    println!("=== Testing Logical Plan Builder ===\n");
    
    println!("--- Test 1: Simple Scan ---");
    let builder = PlanBuilder::new(&catalog);
    match builder.scan("students") {
        Ok(builder) => {
            match builder.build() {
                Ok(plan) => {
                    println!("{}\n", plan);
                },
                Err(e) => println!("Error building plan: {:?}\n", e),
            }
        },
        Err(e) => println!("Error scanning: {:?}\n", e),
    }
    
    println!("--- Test 2: Scan + Filter (IsVerified = true) ---");
    let builder = PlanBuilder::new(&catalog);
    let filter_expr = Expression::Binary {
        left: Box::new(Expression::Column { 
            name: "IsVerified".to_string(), 
            data_type: DataType::Bool 
        }),
        right: Box::new(Expression::Literal(ScalarValue::Bool(Some(true)))),
        operator: Operator::Eq,
    };
    
    match builder.scan("students") {
        Ok(builder) => {
            match builder.filter(filter_expr) {
                Ok(builder) => {
                    match builder.build() {
                        Ok(plan) => {
                            println!("{}\n", plan);
                        },
                        Err(e) => println!("Error building plan: {:?}\n", e),
                    }
                },
                Err(e) => println!("Error filtering: {:?}\n", e),
            }
        },
        Err(e) => println!("Error scanning: {:?}\n", e),
    }
    
    println!("--- Test 3: Scan + Project (Name, Email) ---");
    let builder = PlanBuilder::new(&catalog);
    match builder.scan("students") {
        Ok(builder) => {
            match builder.project(vec!["Name".to_string(), "Email".to_string()]) {
                Ok(builder) => {
                    match builder.build() {
                        Ok(plan) => {
                            println!("{}\n", plan);
                        },
                        Err(e) => println!("Error building plan: {:?}\n", e),
                    }
                },
                Err(e) => println!("Error projecting: {:?}\n", e),
            }
        },
        Err(e) => println!("Error scanning: {:?}\n", e),
    }
    
    println!("--- Test 4: Full Query (Scan + Filter + Project) ---");
    let builder = PlanBuilder::new(&catalog);
    let filter_expr = Expression::Binary {
        left: Box::new(Expression::Column { 
            name: "IsVerified".to_string(), 
            data_type: DataType::Bool 
        }),
        right: Box::new(Expression::Literal(ScalarValue::Bool(Some(true)))),
        operator: Operator::Eq,
    };
    
    match builder.scan("students") {
        Ok(builder) => {
            match builder.filter(filter_expr) {
                Ok(builder) => {
                    match builder.project(vec!["Name".to_string(), "Email".to_string()]) {
                        Ok(builder) => {
                            match builder.build() {
                                Ok(plan) => {
                                    println!("{}\n", plan);
                                },
                                Err(e) => println!("Error building plan: {:?}\n", e),
                            }
                        },
                        Err(e) => println!("Error projecting: {:?}\n", e),
                    }
                },
                Err(e) => println!("Error filtering: {:?}\n", e),
            }
        },
        Err(e) => println!("Error scanning: {:?}\n", e),
    }
    
    println!("=== Tests Complete ===");
}