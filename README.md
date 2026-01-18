# Vektur

Vektur is a lightweight, educational query engine written in Rust from scratch.

It parses and executes SQL queries over in-memory data, CSV files, and Parquet files, demonstrating core database concepts including type systems, expression evaluation, query planning, and vectorized execution.

## Features

- **Custom Type System**: Native implementation of fundamental data types (integers, floats, strings, booleans) with null handling
- **SQL Parser**: Converts SQL queries into an internal representation
- **Query Optimization**: Logical and physical query planning with optimization passes
- **Multiple Data Sources**: 
  - In-memory tables
  - CSV files
  - Parquet files
- **Vectorized Execution**: Columnar batch processing for efficient query execution
- **Expression Engine**: Evaluates complex expressions, filters, and projections

## Architecture

Vektur is built in distinct layers, each responsible for a specific aspect of query processing:

1. **Type System** - Defines data types and schemas
2. **Expression Representation** - Internal representation of computations
3. **Data Sources** - Abstraction over different data formats
4. **SQL Parser** - Converts SQL text into Abstract Syntax Trees
5. **Logical Planning** - Query optimization at the logical level
6. **Physical Planning** - Execution strategy selection
7. **Execution Engine** - Actual query execution and result computation