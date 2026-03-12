use std::rc::Rc;

use vektur::{
    RecordBatch, ScalarValue,
    datasource::csv::CsvDataSource,
    errors::LexerError,
    logical_plan::plan::Catalog,
    physical_plan::planner::create_physical_plan,
    sql_support::sql::{parse_sql, sql_to_logical_plan},
};

fn print_batch(batch: &RecordBatch, print_header: bool) {
    let headers: Vec<&str> = batch.schema.fields.iter().map(|f| f.name.as_str()).collect();

    if print_header {
        println!("{}", headers.join(" | "));
        println!("{}", "-".repeat(headers.join(" | ").len()));
    }

    if batch.columns.is_empty() {
        return;
    }

    let row_count = batch.columns[0].values.len();
    for row_i in 0..row_count {
        let row: Vec<String> = batch
            .columns
            .iter()
            .map(|col| match &col.values[row_i] {
                ScalarValue::Int32(Some(v)) => v.to_string(),
                ScalarValue::Float64(Some(v)) => v.to_string(),
                ScalarValue::Bool(Some(v)) => v.to_string(),
                ScalarValue::String(Some(v)) => v.clone(),
                _ => "NULL".to_string(),
            })
            .collect();
        println!("{}", row.join(" | "));
    }
}

fn execute_sql(sql: &str, catalog: &Catalog) {
    println!("SQL: {}", sql);

    let statements = match parse_sql(sql) {
        Ok(s) => s,
        Err(LexerError::InvalidToken { message }) => {
            println!("Parse error: {}\n", message);
            return;
        }
    };

    let statement = match statements.first() {
        Some(s) => s,
        None => {
            println!("No statements found\n");
            return;
        }
    };

    let logical_plan = match sql_to_logical_plan(statement, catalog) {
        Ok(p) => p,
        Err(e) => {
            println!("Logical plan error: {:?}\n", e);
            return;
        }
    };

    let physical_plan = match create_physical_plan(&logical_plan, catalog) {
        Ok(p) => p,
        Err(e) => {
            println!("Physical plan error: {:?}\n", e);
            return;
        }
    };

    let mut row_count = 0;
    let mut first_batch = true;
    for batch_result in physical_plan.execute() {
        match batch_result {
            Ok(batch) => {
                let batch_rows = if batch.columns.is_empty() {
                    0
                } else {
                    batch.columns[0].values.len()
                };
                print_batch(&batch, first_batch);
                first_batch = false;
                row_count += batch_rows;
            }
            Err(e) => {
                println!("Execution error: {:?}", e);
                return;
            }
        }
    }
    println!("({} rows)\n", row_count);
}

fn main() {
    let mut catalog = Catalog::new();
    match CsvDataSource::new("test/students.csv".to_string()) {
        Ok(csv_source) => {
            catalog.register_table("students".to_string(), Rc::new(csv_source));
        }
        Err(err) => {
            println!("Error loading CSV: {:?}", err);
            return;
        }
    }

    execute_sql(
        "SELECT Name, Email FROM students WHERE IsVerified = true",
        &catalog,
    );
    execute_sql("SELECT * FROM students", &catalog);
    execute_sql(
        "SELECT Name FROM students WHERE \"S/N\" > 50",
        &catalog,
    );
    execute_sql(
        "SELECT Name FROM students WHERE \"S/N\" >= 50",
        &catalog,
    );
    execute_sql(
        "SELECT Name FROM students WHERE \"S/N\" <= 10",
        &catalog,
    );
    execute_sql(
        "SELECT Name FROM students WHERE IsVerified != true",
        &catalog,
    );
    execute_sql(
        "SELECT Name, \"S/N\" FROM students WHERE \"S/N\" > 0",
        &catalog,
    );
}
