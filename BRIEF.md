Vektur — Project Re-entry Brief
                                                                                                    
  What Is This?                                                                                     

  Vektur is a minimal SQL query engine written in Rust. It's not a production database — it's an
  educational project that walks through the core layers of how a database processes a query:
  parsing SQL → building a logical plan → (eventually) executing it against real data. The data
  source is CSV files.

  Think of it as: "read a CSV, run SQL against it, get results."

  ---
  What You've Built So Far

  The frontend and planning layers are complete. Here's the stack:

  Layer 1 — Type System (src/types/)

  - DataType: Int32, Float64, String, Bool
  - ScalarValue: nullable wrapper around each type
  - ColumnVector: columnar storage (Vec<ScalarValue>)
  - Schema, Field, RecordBatch: schema-aware batches of columnar data

  Layer 2 — Data Source (src/datasource/)

  - DataSource trait with schema() and scan() methods
  - CsvDataSource: reads CSV files in batches of 16 rows, auto-infers schema from the first 101
  rows, handles type detection and nullability

  Layer 3 — Logical Plan (src/logical_plan/)

  - Expression: Column, Literal, Binary (arithmetic + comparison), Unary (NOT, IS NULL, Negate)
  - LogicalPlan: Scan, Filter, Projection
  - PlanBuilder: builder-pattern for chaining plan steps with type validation at each step
  - ExecutionContext + Catalog: registers CSV tables by name, returns a DataFrame
  - DataFrame: wraps a LogicalPlan, exposes .project(), .filter(), .build()

  Layer 4 — SQL Frontend (src/sql_support/)

  - SQL string → sqlparser AST → your internal LogicalPlan
  - Handles SELECT col1, col2 FROM table WHERE expr
  - Handles SELECT * (expands to all columns)
  - Supports all comparison + arithmetic operators

  CLI (src/cli/)

  - clap-based arg parsing for --file and --query paths (wired up but not fully plumbed to
  execution)

  ---
  What's Left

  The entire backend — nothing executes yet. The plan is built but never evaluated against data.

  SQL String
     ↓  [DONE] sqlparser
  AST
     ↓  [DONE] planner.rs
  LogicalPlan
     ↓  [TODO] physical planner       ← YOU ARE HERE
  PhysicalPlan
     ↓  [TODO] execution engine
  RecordBatch results
     ↓  [TODO] output/display
  Printed results

  ---
  How to Test What's Done Now

  You can verify the planning layer works today:

  # Build
  cargo build

  # Run the demo (main.rs has hardcoded test queries)
  cargo run

  # Test files are in:
  # test/students.csv  — 200 student records
  # test/users.csv

  main.rs currently runs several hardcoded SQL queries through the parser + planner and prints the
  resulting LogicalPlan tree. No rows are returned yet — that's the next step. If it prints an
  indented plan tree without panicking, the frontend is working.

  ---
  How to Test Going Forward

  Since there are no tests at all right now, you should add them as you build. Recommended approach:

  # Run tests
  cargo test

  # Run a specific test module
  cargo test physical_plan

  # Run with output visible
  cargo test -- --nocapture

  Add tests in the relevant src/ files using #[cfg(test)] blocks, or create
  tests/integration_test.rs for end-to-end tests. A good first integration test: "given this CSV and
   this SQL, assert these rows come back."

  ---
  Next Steps — The Physical Execution Layer

  You confirmed you were about to start the physical planner. Here's the concrete TODO list:

  Phase 1 — Physical Plan (start here)

  - Create src/physical_plan/mod.rs
  - Define a PhysicalPlan enum mirroring LogicalPlan:
    - PhysicalScan { source: Box<dyn DataSource>, projection: Option<Vec<usize>> }
    - PhysicalFilter { input: Box<PhysicalPlan>, predicate: Expression }
    - PhysicalProjection { input: Box<PhysicalPlan>, exprs: Vec<Expression> }
  - Define a PhysicalPlanner that converts LogicalPlan → PhysicalPlan
    - This is mostly a 1:1 mapping for now (no optimization yet)

  Phase 2 — Expression Evaluator

  - Create src/physical_plan/eval.rs
  - Implement evaluate(expr: &Expression, batch: &RecordBatch) -> ColumnVector
    - Column → look up column by name in batch, return its ColumnVector
    - Literal → repeat the scalar value N times (N = batch row count)
    - Binary → element-wise operation on two ColumnVectors
    - Unary → element-wise negation / null check
  - Add evaluate_predicate(expr, batch) -> Vec<bool> for filter masks

  Phase 3 — Execution

  - Give PhysicalPlan an execute() -> impl Iterator<Item = RecordBatch> method
    - PhysicalScan::execute() → calls DataSource::scan()
    - PhysicalFilter::execute() → pulls batches from input, applies boolean mask row-by-row
    - PhysicalProjection::execute() → pulls batches, evaluates each projection expression
  - Wire ExecutionContext::execute(plan: LogicalPlan) to call the physical planner then execute

  Phase 4 — Output + Wire Up CLI

  - Implement Display or a print helper for RecordBatch (table-style output)
  - Plumb src/main.rs to actually call execute and print results
  - Wire CLI args (--file, --query) to the full pipeline

  Phase 5 — Cleanup & Tests (do this throughout)

  - Delete or reconcile src/logical_plan/former_plan.rs (looks like dead code)
  - Fix the todo!() in PlanBuilder::project() for non-column expressions
  - Fix the NULL → String(None) hack in sql_value_to_scalar()
  - Add unit tests for expression evaluator
  - Add integration tests: CSV + SQL → expected rows

  ---
  Scope Reminder

  You said it yourself — this is not a full engine. Explicitly out of scope:

  - JOINs
  - Aggregations (GROUP BY, COUNT, SUM)
  - ORDER BY / LIMIT
  - INSERT / UPDATE / DELETE
  - Query optimization / predicate pushdown
  - Parquet support

  The goal is: SQL SELECT with WHERE and column projection works end-to-end on a CSV. Everything
  above Phase 4 gets you there.
