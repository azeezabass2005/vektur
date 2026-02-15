use vektur::{
    datasource::csv::CsvDataSource,
    errors::LexerError,
    logical_plan::plan::Catalog,
    sql_support::sql::{parse_sql, sql_to_logical_plan},
};

fn test_sql_query(sql: &str, catalog: &Catalog) {
    println!("SQL: {}", sql);
    match parse_sql(sql) {
        Ok(statements) => {
            if let Some(statement) = statements.first() {
                match sql_to_logical_plan(statement, catalog) {
                    Ok(plan) => println!("{}\n", plan),
                    Err(e) => println!("Error: {:?}\n", e),
                }
            }
        }
        Err(LexerError::InvalidToken { message }) => {
            println!("Parse error: {}\n", message);
        }
    }
}

fn main() {
    let mut catalog = Catalog::new();
    match CsvDataSource::new("test/students.csv".to_string()) {
        Ok(csv_source) => {
            catalog.register_table("students".to_string(), Box::new(csv_source));
        }
        Err(err) => {
            println!("Error loading CSV: {:?}", err);
            return;
        }
    }

    test_sql_query("SELECT Name, Email FROM students WHERE IsVerified = true", &catalog);
    test_sql_query("SELECT * FROM students", &catalog);
    test_sql_query("SELECT Name FROM students WHERE \"S/N\" > 50", &catalog);
    test_sql_query("SELECT Name FROM students WHERE \"S/N\" >= 50", &catalog);
    test_sql_query("SELECT Name FROM students WHERE \"S/N\" <= 10", &catalog);
    test_sql_query("SELECT Name FROM students WHERE IsVerified != true", &catalog);
    test_sql_query("SELECT Name, \"S/N\" FROM students WHERE \"S/N\" > 0", &catalog);
    test_sql_query("SELECT *, Name FROM students", &catalog);
}