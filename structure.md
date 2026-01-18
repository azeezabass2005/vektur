query-engine/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── main.rs
│   │
│   ├── types/           # Phase 1: Type system
│   │   ├── mod.rs
│   │   ├── datatype.rs
│   │   ├── scalar.rs
│   │   └── schema.rs
│   │
│   ├── expr/            # Phase 2: Expression representation
│   │   ├── mod.rs
│   │   ├── expr.rs      # Expression enum and tree structure
│   │   ├── literal.rs   # Literal values
│   │   ├── binary.rs    # Binary operations
│   │   ├── function.rs  # Function calls
│   │   └── type_check.rs # Type inference/checking for expressions
│   │
│   ├── datasource/      # Phase 3: Data sources
│   │   ├── mod.rs
│   │   ├── source.rs    # DataSource trait
│   │   ├── memory.rs    # In-memory table implementation
│   │   ├── csv.rs       # CSV file source
│   │   └── batch.rs     # RecordBatch (chunk of rows)
│   │
│   ├── parser/          # Phase 4: SQL parsing
│   │   ├── mod.rs
│   │   ├── lexer.rs     # Tokenization
│   │   ├── parser.rs    # Parse tokens → AST
│   │   └── ast.rs       # AST node definitions
│   │
│   ├── logical/         # Phase 5: Logical planning
│   │   ├── mod.rs
│   │   ├── plan.rs      # Logical plan nodes
│   │   ├── optimizer/   # Optimization rules
│   │   │   ├── mod.rs
│   │   │   ├── predicate_pushdown.rs
│   │   │   ├── projection_pushdown.rs
│   │   │   └── constant_folding.rs
│   │   └── planner.rs   # AST → Logical plan conversion
│   │
│   ├── physical/        # Phase 6: Physical planning
│   │   ├── mod.rs
│   │   ├── plan.rs      # Physical plan nodes
│   │   └── planner.rs   # Logical → Physical conversion
│   │
│   ├── execution/       # Phase 7: Execution engine
│   │   ├── mod.rs
│   │   ├── context.rs   # Query execution context
│   │   ├── operators/   # Physical operators
│   │   │   ├── mod.rs
│   │   │   ├── scan.rs
│   │   │   ├── filter.rs
│   │   │   ├── project.rs
│   │   │   ├── hash_join.rs
│   │   │   └── aggregate.rs
│   │   └── eval.rs      # Expression evaluator
│   │
│   ├── error.rs         # Error types (add early!)
│   └── utils.rs         # Shared utilities
│
└── tests/
    ├── integration/     # End-to-end query tests
    │   └── queries.rs
    └── unit/            # Unit tests per module
        ├── types_test.rs
        ├── expr_test.rs
        └── ...