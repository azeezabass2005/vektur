# Physical Plans and Expressions — Vektur Guide

This document is a Rust-translated reference of the physical plan layer, adapted
specifically to Vektur's type system. Everything here uses the exact types already
in your codebase (`ScalarValue`, `ColumnVector`, `RecordBatch`, `Schema`, etc.).
Read it, understand it, then close it and implement from the todo list at the bottom.

---

## Why Separate Physical from Logical?

Your `LogicalPlan` describes *what* to compute:

```
Projection: ["Name", "Email"]
  Filter: S/N > 50
    Scan: students.csv
```

It says nothing about *how*. A physical plan is the "how" — it is executable code.
For your scope (SELECT + WHERE on CSV), this separation is simpler than it sounds:
logical and physical plans map almost 1-to-1. But the separation matters because
the physical layer is where real data flows.

---

## The Core Trait

Every physical operator implements the same trait. It produces `RecordBatch`es on
demand — the caller pulls batches rather than having them pushed.

```rust
pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;

    // Returns an iterator that yields batches one at a time.
    // Using Box<dyn Iterator> lets each operator produce data lazily.
    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>>;

    // Used for inspecting the plan tree (printing, debugging, optimization later).
    fn children(&self) -> Vec<&dyn PhysicalPlan>;
}
```

The `execute()` method returns an iterator, not a `Vec`. This is important:
if a query only needs 10 rows, you do not want to read the entire CSV first.
Rust's `impl Iterator` is lazy by default — nothing runs until `.next()` is called.

---

## Physical Expressions

A physical expression takes a `RecordBatch` and produces a `ColumnVector` —
one value per row in the batch.

```rust
pub trait PhysicalExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError>;
}
```

Compare this to your `Expression` in `logical_plan/plan.rs`. The logical expression
holds column names. The physical expression works with column **indexes** — no name
lookups at runtime.

### Column Expression

Retrieves one column from the batch by its position index.

```rust
pub struct ColumnExpr {
    pub index: usize,
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        // batch.columns is Vec<ColumnVector> — just clone the one at `index`
        // (or return a reference if you want to avoid the clone)
        Ok(batch.columns[self.index].clone())
    }
}
```

No computation here. Just a direct lookup. This is why the physical layer uses
indexes instead of names — you resolve the name once during planning, never again
at execution time.

### Literal Expression

A literal like `50` or `true` must produce a column of the same value repeated
once per row. This is needed so binary expressions can work element-wise on two
equally-sized `ColumnVector`s.

```rust
pub struct LiteralExpr {
    pub value: ScalarValue,
}

impl PhysicalExpr for LiteralExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        // How many rows are in this batch?
        let row_count = batch.columns[0].values.len();

        // Repeat the scalar value once per row
        let values = (0..row_count)
            .map(|_| self.value.clone())
            .collect();

        Ok(ColumnVector::new(values))
    }
}
```

### Binary Expression

A binary expression has a left child, a right child, and an operator.
It evaluates both sides first, then applies the operator element-wise.

```rust
pub struct BinaryExpr {
    pub left:  Box<dyn PhysicalExpr>,
    pub right: Box<dyn PhysicalExpr>,
    pub op:    Operator,  // reuse your existing Operator enum
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        let left_col  = self.left.evaluate(batch)?;
        let right_col = self.right.evaluate(batch)?;

        // Both columns must have the same length
        assert_eq!(left_col.values.len(), right_col.values.len());

        // Apply the operator to each pair (left[i], right[i])
        let values = left_col.values.iter()
            .zip(right_col.values.iter())
            .map(|(l, r)| apply_operator(l, r, &self.op))
            .collect::<Result<Vec<ScalarValue>, QueryError>>()?;

        Ok(ColumnVector::new(values))
    }
}

// A helper that applies one operator to one pair of ScalarValues.
// This is where all the type-matching happens.
fn apply_operator(
    left:  &ScalarValue,
    right: &ScalarValue,
    op:    &Operator,
) -> Result<ScalarValue, QueryError> {
    match (left, right) {
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
            let result = match op {
                Operator::Add      => ScalarValue::Int32(Some(l + r)),
                Operator::Subtract => ScalarValue::Int32(Some(l - r)),
                Operator::Multiply => ScalarValue::Int32(Some(l * r)),
                Operator::Divide   => ScalarValue::Int32(Some(l / r)),
                Operator::Eq       => ScalarValue::Bool(Some(l == r)),
                Operator::NotEq    => ScalarValue::Bool(Some(l != r)),
                Operator::Gt       => ScalarValue::Bool(Some(l > r)),
                Operator::Lt       => ScalarValue::Bool(Some(l < r)),
                Operator::GtEq     => ScalarValue::Bool(Some(l >= r)),
                Operator::LtEq     => ScalarValue::Bool(Some(l <= r)),
            };
            Ok(result)
        },
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            // same pattern...
            todo!()
        },
        // Handle Int32 vs Float64 mixed arithmetic by promoting to Float64
        // Handle null on either side → return null
        _ => Err(QueryError::TypeMismatch),
    }
}
```

### Unary Expression

Applies a single-operand operator to each value in a column.

```rust
pub struct UnaryExpr {
    pub operand: Box<dyn PhysicalExpr>,
    pub op:      UnaryOperator,  // reuse your existing UnaryOperator enum
}

impl PhysicalExpr for UnaryExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError> {
        let col = self.operand.evaluate(batch)?;

        let values = col.values.iter().map(|v| {
            match (&self.op, v) {
                (UnaryOperator::Not, ScalarValue::Bool(b)) =>
                    ScalarValue::Bool(b.map(|x| !x)),
                (UnaryOperator::Negate, ScalarValue::Int32(n)) =>
                    ScalarValue::Int32(n.map(|x| -x)),
                (UnaryOperator::Negate, ScalarValue::Float64(n)) =>
                    ScalarValue::Float64(n.map(|x| -x)),
                (UnaryOperator::IsNull, v) =>
                    ScalarValue::Bool(Some(matches!(v,
                        ScalarValue::Int32(None)
                        | ScalarValue::Float64(None)
                        | ScalarValue::String(None)
                        | ScalarValue::Bool(None)
                    ))),
                (UnaryOperator::IsNotNull, v) =>
                    // opposite of IsNull
                    todo!(),
                _ => v.clone(),
            }
        }).collect();

        Ok(ColumnVector::new(values))
    }
}
```

---

## Physical Plan Operators

### ScanExec

Reads from a `DataSource`. Delegates everything to the data source, just like
`LogicalPlan::Scan` describes.

```rust
pub struct ScanExec {
    pub source:     Box<dyn DataSource>,
    pub schema:     Schema,
    pub projection: Option<Vec<String>>,  // which columns to read
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>> {
        // DataSource::scan() already returns an iterator of RecordBatch results.
        // Wrap it in a Box so it fits the return type.
        Box::new(self.source.scan())
    }

    fn children(&self) -> Vec<&dyn PhysicalPlan> {
        vec![]  // leaf node — no children
    }
}
```

### ProjectionExec

For each batch from its input, evaluates each expression to build a new batch.
If the expression is just a `ColumnExpr`, no data is copied — it is a direct
lookup. Computed expressions (like `salary * 1.1`) produce new columns.

```rust
pub struct ProjectionExec {
    pub input:  Box<dyn PhysicalPlan>,
    pub schema: Schema,
    pub exprs:  Vec<Box<dyn PhysicalExpr>>,
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>> {
        let input_iter = self.input.execute();
        let exprs      = &self.exprs;
        let schema     = self.schema.clone();

        // For each batch coming from the input:
        Box::new(input_iter.map(move |batch_result| {
            let batch = batch_result?;

            // Evaluate every expression against this batch
            let columns: Result<Vec<ColumnVector>, _> = exprs
                .iter()
                .map(|expr| expr.evaluate(&batch))
                .collect();

            RecordBatch::new(schema.clone(), columns?)
        }))
    }

    fn children(&self) -> Vec<&dyn PhysicalPlan> {
        vec![self.input.as_ref()]
    }
}
```

### FilterExec (Selection)

Evaluates a predicate expression to get a boolean column, then keeps only the
rows where the value is `Bool(Some(true))`.

```rust
pub struct FilterExec {
    pub input:     Box<dyn PhysicalPlan>,
    pub predicate: Box<dyn PhysicalExpr>,
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>> {
        let input_iter = self.input.execute();
        let predicate  = &self.predicate;

        Box::new(input_iter.map(move |batch_result| {
            let batch = batch_result?;

            // Step 1: evaluate the predicate — produces one Bool per row
            let bool_col = predicate.evaluate(&batch)?;

            // Step 2: build a bitmask — which row indexes pass the filter?
            let mask: Vec<bool> = bool_col.values.iter().map(|v| {
                matches!(v, ScalarValue::Bool(Some(true)))
            }).collect();

            // Step 3: for every column, keep only the rows where mask[i] == true
            let filtered_columns: Vec<ColumnVector> = batch.columns.iter().map(|col| {
                let kept: Vec<ScalarValue> = col.values.iter()
                    .zip(mask.iter())
                    .filter(|(_, &keep)| keep)
                    .map(|(val, _)| val.clone())
                    .collect();
                ColumnVector::new(kept)
            }).collect();

            RecordBatch::new(batch.schema.clone(), filtered_columns)
        }))
    }

    fn children(&self) -> Vec<&dyn PhysicalPlan> {
        vec![self.input.as_ref()]
    }
}
```

---

## The Physical Planner

The physical planner walks a `LogicalPlan` tree and converts it into a
`PhysicalPlan` tree. For this project it is a straightforward 1-to-1 conversion.
The interesting part is resolving column **names** (logical) into column **indexes**
(physical) — you do that here once, so the executor never has to search by name.

```rust
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn create_physical_plan(
        plan: &LogicalPlan,
        catalog: &Catalog,
    ) -> Result<Box<dyn PhysicalPlan>, QueryError> {
        match plan {
            LogicalPlan::Scan { path, schema, projection } => {
                let source = catalog.get_source(path)?;
                Ok(Box::new(ScanExec {
                    source,
                    schema: schema.clone(),
                    projection: projection.clone(),
                }))
            },

            LogicalPlan::Filter { input, predicate } => {
                let physical_input = Self::create_physical_plan(input, catalog)?;
                let input_schema   = physical_input.schema().clone();

                // Resolve the logical expression into a physical expression
                // using the input schema to turn column names into indexes
                let physical_predicate = Self::create_physical_expr(predicate, &input_schema)?;

                Ok(Box::new(FilterExec {
                    input:     physical_input,
                    predicate: physical_predicate,
                }))
            },

            LogicalPlan::Projection { input, columns } => {
                let physical_input = Self::create_physical_plan(input, catalog)?;
                let input_schema   = physical_input.schema().clone();

                let physical_exprs: Result<Vec<_>, _> = columns.iter()
                    .map(|expr| Self::create_physical_expr(expr, &input_schema))
                    .collect();

                // Build the output schema from the projected columns
                let output_schema = /* derive from columns and input_schema */ todo!();

                Ok(Box::new(ProjectionExec {
                    input:  physical_input,
                    schema: output_schema,
                    exprs:  physical_exprs?,
                }))
            },
        }
    }

    // Converts a logical Expression into a physical PhysicalExpr.
    // The key job here is: look up column names in the schema and
    // replace them with index-based ColumnExpr.
    fn create_physical_expr(
        expr:   &Expression,
        schema: &Schema,
    ) -> Result<Box<dyn PhysicalExpr>, QueryError> {
        match expr {
            Expression::Column { name, .. } => {
                // Find the index of this column in the schema
                let index = schema.fields.iter()
                    .position(|f| &f.name == name)
                    .ok_or_else(|| QueryError::ValidationError {
                        message: format!("Column '{}' not found", name),
                    })?;
                Ok(Box::new(ColumnExpr { index }))
            },

            Expression::Literal(scalar) => {
                Ok(Box::new(LiteralExpr { value: scalar.clone() }))
            },

            Expression::Binary { left, right, operator } => {
                let l = Self::create_physical_expr(left,  schema)?;
                let r = Self::create_physical_expr(right, schema)?;
                Ok(Box::new(BinaryExpr { left: l, right: r, op: operator.clone() }))
            },

            Expression::Unary { operand, operator } => {
                let o = Self::create_physical_expr(operand, schema)?;
                Ok(Box::new(UnaryExpr { operand: o, op: operator.clone() }))
            },
        }
    }
}
```

---

## Execution Flow — End to End

Once everything is wired up, a full query executes like this:

```
SQL string
   ↓  sql_support/sql.rs      → parse to AST
   ↓  sql_support/planner.rs  → LogicalPlan
   ↓  PhysicalPlanner         → PhysicalPlan (Box<dyn PhysicalPlan>)
   ↓  .execute()              → Iterator<Item = RecordBatch>
   ↓  print loop              → rows on screen
```

In code:

```rust
// In ExecutionContext or main.rs
let logical_plan  = sql_to_logical_plan(&sql, &catalog)?;
let physical_plan = PhysicalPlanner::create_physical_plan(&logical_plan, &catalog)?;

for batch_result in physical_plan.execute() {
    let batch = batch_result?;
    print_batch(&batch);
}
```

---

## Printing Results

A simple table printer for a `RecordBatch`:

```rust
fn print_batch(batch: &RecordBatch) {
    // Print header
    let headers: Vec<&str> = batch.schema.fields.iter()
        .map(|f| f.name.as_str())
        .collect();
    println!("{}", headers.join(" | "));
    println!("{}", "-".repeat(headers.join(" | ").len()));

    // How many rows?
    if batch.columns.is_empty() { return; }
    let row_count = batch.columns[0].values.len();

    for row_i in 0..row_count {
        let row: Vec<String> = batch.columns.iter().map(|col| {
            match &col.values[row_i] {
                ScalarValue::Int32(Some(v))   => v.to_string(),
                ScalarValue::Float64(Some(v)) => v.to_string(),
                ScalarValue::Bool(Some(v))    => v.to_string(),
                ScalarValue::String(Some(v))  => v.clone(),
                _                             => "NULL".to_string(),
            }
        }).collect();
        println!("{}", row.join(" | "));
    }
}
```

---

## Execution Model Note

Vektur uses **pull-based** execution. The root operator calls `.execute()` on its
child, which calls `.execute()` on its child, and so on down to the scan.
Data flows upward one batch at a time only when the caller asks for the next batch.

This is the iterator model: nothing runs until you call `.next()`. A `Filter` does
not read the whole CSV upfront — it reads one batch, filters it, hands it up, then
reads the next batch only when asked. This is why returning `Box<dyn Iterator>`
(or `impl Iterator`) matters.

---

---

# TODO — Build the Physical Planner Yourself

Work through these in order. Each task is small enough to implement in one sitting.
Do not skip ahead — each step builds on the previous one.

---

## Step 1 — Create the module skeleton

- [ ] Create `src/physical_plan/mod.rs`
- [ ] Create `src/physical_plan/expr.rs`
- [ ] Create `src/physical_plan/planner.rs`
- [ ] Add `pub mod physical_plan;` to `src/lib.rs`
- [ ] Re-export the public types from `mod.rs` with `pub use`

---

## Step 2 — Define the `PhysicalPlan` trait

In `src/physical_plan/mod.rs`:

- [ ] Define the `PhysicalPlan` trait with three methods:
  - `fn schema(&self) -> &Schema;`
  - `fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>>>;`
  - `fn children(&self) -> Vec<&dyn PhysicalPlan>;`
- [ ] Confirm the trait compiles with a dummy empty struct that implements it

---

## Step 3 — Define the `PhysicalExpr` trait

In `src/physical_plan/expr.rs`:

- [ ] Define the `PhysicalExpr` trait:
  - `fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnVector, QueryError>;`
- [ ] Confirm it compiles

---

## Step 4 — Implement `ColumnExpr`

- [ ] Create struct `ColumnExpr { index: usize }`
- [ ] Implement `PhysicalExpr` for it
  - Access `batch.columns[self.index]` and return a clone of it
- [ ] Write a small test: create a `RecordBatch` manually with known values, call
  `ColumnExpr { index: 0 }.evaluate(&batch)`, assert the output matches

---

## Step 5 — Implement `LiteralExpr`

- [ ] Create struct `LiteralExpr { value: ScalarValue }`
- [ ] Implement `PhysicalExpr`:
  - Count rows from `batch.columns[0].values.len()`
  - Return a `ColumnVector` with that value repeated N times
- [ ] Handle the edge case: what if the batch has no columns (0 rows)?
- [ ] Write a test: 5-row batch, `LiteralExpr { Int32(Some(42)) }` → vector of five `42`s

---

## Step 6 — Implement `BinaryExpr` — comparison operators

- [ ] Create struct `BinaryExpr { left, right, op: Operator }`
  - Both `left` and `right` are `Box<dyn PhysicalExpr>`
- [ ] Implement `PhysicalExpr`:
  - Evaluate left and right
  - Assert both output columns have the same length
  - Zip them and apply the operator element-wise using a helper function
- [ ] Write the helper `apply_operator(l: &ScalarValue, r: &ScalarValue, op: &Operator) -> Result<ScalarValue, QueryError>`
- [ ] Handle only comparison operators first (`Eq`, `NotEq`, `Gt`, `Lt`, `GtEq`, `LtEq`):
  - `(Int32(Some(l)), Int32(Some(r)))` — all six comparisons → `Bool(Some(...))`
  - `(Bool(Some(l)), Bool(Some(r)))` — `Eq` and `NotEq` only → `Bool(Some(...))`
  - `(String(Some(l)), String(Some(r)))` — `Eq` and `NotEq` → `Bool(Some(...))`
  - Any `None` on either side → return `Bool(None)`
- [ ] Write a test: column `[1, 2, 3]` vs literal `2`, operator `Gt` → `[false, false, true]`
- [ ] Write a test: column `[true, false, true]` vs literal `true`, operator `Eq` → `[true, false, true]`

---

## Step 7 — Implement `BinaryExpr` — arithmetic operators

- [ ] Extend `apply_operator` to handle `Add`, `Subtract`, `Multiply`, `Divide`:
  - `(Int32(Some(l)), Int32(Some(r)))` → `Int32(Some(result))`
  - `(Float64(Some(l)), Float64(Some(r)))` → `Float64(Some(result))`
  - `(Int32(Some(l)), Float64(Some(r)))` → promote `l` to `f64`, return `Float64`
  - `(Float64(Some(l)), Int32(Some(r)))` → same, other direction
  - Division by zero: decide your behaviour (return `None`? return an error?)
  - Any `None` on either side → return `None` of the result type
- [ ] Write a test: `[10, 20, 30]` plus literal `5` → `[15, 25, 35]`
- [ ] Write a test: `[10, 0, 5]` divided by literal `0` → check your chosen behaviour

---

## Step 8 — Implement `UnaryExpr`

- [ ] Create struct `UnaryExpr { operand: Box<dyn PhysicalExpr>, op: UnaryOperator }`
- [ ] Implement `PhysicalExpr` — map each value:
  - `Not` on `Bool(Some(b))` → `Bool(Some(!b))`
  - `Not` on `Bool(None)` → `Bool(None)`
  - `Negate` on `Int32(Some(n))` → `Int32(Some(-n))`
  - `Negate` on `Float64(Some(n))` → `Float64(Some(-n))`
  - `IsNull` → `Bool(Some(true))` if the inner `Option` is `None`, else `Bool(Some(false))`
  - `IsNotNull` → opposite of `IsNull`
- [ ] Write a test for `Not` on a mixed bool column
- [ ] Write a test for `IsNull` on a column with some `None` entries

---

## Step 9 — Implement `ScanExec`

In `src/physical_plan/mod.rs` (or a `src/physical_plan/scan.rs`):

- [ ] Create struct `ScanExec` holding a `Box<dyn DataSource>` and a `Schema`
- [ ] Implement `PhysicalPlan`:
  - `schema()` → return `&self.schema`
  - `execute()` → call `self.source.scan()` and wrap it in a `Box`
  - `children()` → return empty `vec![]`
- [ ] You will need to look at what `DataSource::scan()` currently returns and
  make sure the iterator item type matches `Result<RecordBatch, QueryError>`
  (it may already, or you may need a small `.map()` wrapper)

---

## Step 10 — Implement `FilterExec`

- [ ] Create struct `FilterExec { input: Box<dyn PhysicalPlan>, predicate: Box<dyn PhysicalExpr> }`
- [ ] Implement `PhysicalPlan`:
  - `schema()` → delegate to `self.input.schema()`
  - `children()` → `vec![self.input.as_ref()]`
  - `execute()`:
    1. Call `self.input.execute()` to get the input iterator
    2. Map each batch:
       a. Evaluate the predicate → `ColumnVector` of `ScalarValue::Bool`
       b. Build a `Vec<bool>` mask: `true` where value is `Bool(Some(true))`
       c. For each column in the batch, keep only values at positions where the mask is `true`
       d. Assemble a new `RecordBatch` with the same schema and the filtered columns
- [ ] Test with an explicit `RecordBatch`: 5 rows, filter keeps rows where column 0 > 2
  → assert only matching rows come out

---

## Step 11 — Implement `ProjectionExec`

- [ ] Create struct `ProjectionExec { input, schema, exprs: Vec<Box<dyn PhysicalExpr>> }`
- [ ] Implement `PhysicalPlan`:
  - `schema()` → `&self.schema`
  - `children()` → `vec![self.input.as_ref()]`
  - `execute()`:
    1. Get the input iterator
    2. Map each batch:
       a. For each expression in `self.exprs`, call `.evaluate(&batch)`
       b. Collect results into `Vec<ColumnVector>`
       c. Build a new `RecordBatch` with `self.schema` and those columns
- [ ] Test: batch with columns A, B, C — project only B and C → output has 2 columns

---

## Step 12 — Implement the `PhysicalPlanner`

In `src/physical_plan/planner.rs`:

This is the bridge between the logical and physical worlds.

- [ ] Create struct `PhysicalPlanner` (no fields needed)
- [ ] Implement `fn create_physical_plan(plan: &LogicalPlan, catalog: &Catalog) -> Result<Box<dyn PhysicalPlan>, QueryError>`:
  - Match on each `LogicalPlan` variant
  - `Scan` → look up the data source from the catalog by path, build `ScanExec`
  - `Filter` → recurse to build the input, then call `create_physical_expr` for the predicate, build `FilterExec`
  - `Projection` → recurse for input, convert each expression, derive the output schema, build `ProjectionExec`
- [ ] Implement `fn create_physical_expr(expr: &Expression, schema: &Schema) -> Result<Box<dyn PhysicalExpr>, QueryError>`:
  - `Column { name, .. }` → find the column's index in `schema.fields` using `.position()`, build `ColumnExpr`
  - `Literal(scalar)` → build `LiteralExpr`
  - `Binary { left, right, operator }` → recurse on both sides, build `BinaryExpr`
  - `Unary { operand, operator }` → recurse, build `UnaryExpr`

---

## Step 13 — Update `Catalog` to return data sources

The `PhysicalPlanner` needs to retrieve an actual `Box<dyn DataSource>` from the
catalog (not just a schema). Right now `Catalog::get_schema()` only returns a schema.

- [ ] Add a method `get_source(&self, name: &str) -> Result<Box<dyn DataSource>, QueryError>`
  — or find another way to make the data source available to the physical planner
  (e.g., pass it differently, or store the path in `ScanExec` and open the CSV there)
- [ ] Decide: does `ScanExec` hold a `Box<dyn DataSource>` (already open) or a path
  string that it opens lazily? Either works. Pick one and be consistent.

---

## Step 14 — Wire everything up in `ExecutionContext`

- [ ] Add a method `execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, QueryError>` (or return an iterator):
  1. Parse SQL to logical plan using your existing `sql_support` code
  2. Call `PhysicalPlanner::create_physical_plan(&logical_plan, &self.catalog)`
  3. Call `.execute()` on the result
  4. Collect or iterate the batches
- [ ] Update `main.rs` to call this method and print the results using `print_batch`

---

## Step 15 — Print results

- [ ] Write a `print_batch(batch: &RecordBatch)` function (in `main.rs` or a new `src/display.rs`)
- [ ] Print column headers separated by ` | `
- [ ] Print a separator line
- [ ] For each row, print each value formatted as a string
- [ ] Print a summary: `N rows` at the end

---

## Step 16 — Test everything end-to-end

- [ ] Run this query manually and verify the output is correct:
  ```sql
  SELECT Name, "S/N" FROM students WHERE "S/N" > 50
  ```
- [ ] Verify row count matches what you expect from the CSV
- [ ] Run `SELECT * FROM students` and confirm all 200 rows appear
- [ ] Run `SELECT Name FROM students WHERE IsVerified != true` and spot-check a few rows

---

## Step 17 — Add unit tests

- [ ] Test `ColumnExpr` in isolation
- [ ] Test `LiteralExpr` in isolation
- [ ] Test `BinaryExpr` for each operator category
- [ ] Test `FilterExec` with a hand-built `RecordBatch`
- [ ] Test `ProjectionExec` with a hand-built `RecordBatch`
- [ ] Add an integration test in `tests/integration_test.rs`:
  - Parse and execute a SQL query against `test/students.csv`
  - Assert the row count and spot-check specific values

---

## Step 18 — Clean up

- [ ] Delete or empty out `src/logical_plan/former_plan.rs` if it is dead code
- [ ] Remove unused `#[allow(unused)]` suppressions or silence the real warnings
- [ ] Fix the `todo!()` in `PlanBuilder::project()` for non-column expressions
  (just return an error for now — you do not need computed projections)
- [ ] Fix the NULL hack in `sql_support/planner.rs` `sql_value_to_scalar()` where
  SQL `NULL` is mapped to `String(None)` — map it to the correct type based on context,
  or return a dedicated error for now

---

## Out of Scope (do not implement)

These are explicitly not goals for this project:

- JOINs of any kind
- `GROUP BY`, `COUNT`, `SUM`, `AVG`, or any aggregation
- `ORDER BY` / `LIMIT` / `OFFSET`
- `INSERT` / `UPDATE` / `DELETE`
- Subqueries
- Query optimization (predicate pushdown, projection pushdown)
- Parquet or any non-CSV data source
- Parallel or distributed execution
