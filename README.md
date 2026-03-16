# Vektur

A tiny SQL query engine I'm building in Rust to learn how databases actually work under the hood.

Right now it can parse SQL, plan a query, and run it against a CSV file. That's it. No JOINs, no aggregations, no fancy optimizer — just the bones of how a query goes from text to rows.

## Try it

```
cargo run
```

There are some hardcoded queries in `main.rs` that run against `test/students.csv`. You'll see output like:

```
SQL: SELECT Name, Email FROM students WHERE IsVerified = true
Name | Email
------------
Student 1 | student1@gmail.com
Student 2 | student2@gmail.com
...
(150 rows)
```

## What works

- `SELECT` with specific columns or `*`
- `WHERE` with `=`, `!=`, `>`, `<`, `>=`, `<=`
- Arithmetic (`+`, `-`, `*`, `/`)
- `NOT`, `IS NULL`, `IS NOT NULL`
- Types: integers, floats, strings, booleans (all nullable)
- CSV files with automatic schema inference

## How the pipeline works

```
SQL string  -->  AST  -->  LogicalPlan  -->  PhysicalPlan  -->  rows
               (parse)    (what to do)     (how to do it)    (do it)
```

The logical plan is a tree of operations (scan, filter, project) that works with column names. The physical planner turns that into executable code where columns are referenced by index instead of name. Execution is pull-based — each operator lazily pulls batches from the one below it.

## Files

```
src/
  types/           -- DataType, ScalarValue, ColumnVector, Schema, RecordBatch
  datasource/      -- DataSource trait + CSV reader (reads in batches of 16 rows)
  logical_plan/    -- Expression, LogicalPlan, PlanBuilder, Catalog
  sql_support/     -- SQL parsing (via sqlparser) and conversion to LogicalPlan
  physical_plan/   -- PhysicalExpr, PhysicalPlan, and the planner that bridges
                      logical -> physical
  errors.rs
  main.rs

test/
  students.csv     -- 200 rows of fake student data
  users.csv
```

## Not doing

No JOINs, no GROUP BY/aggregation, no ORDER BY/LIMIT, no subqueries, no writes, no query optimization, no data sources other than CSV. Maybe someday, but the point is to understand the fundamentals first.
