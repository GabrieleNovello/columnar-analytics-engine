# Columnar Data Engine

Minimal column-oriented data processing engine focused on storage layout and
vectorized execution.

Important : The project intentionally limits the scope by omitting SQL, query planning, 
            joins, concurrency, and durability.

---

## Repository Structure

```

ingestion/    CSV ingestion and column file generation
metadata/     Table metadata (schema, row count)
execution/    Execution engine and physical operators

```

### Binaries

- `ingest.exe` : CSV -> column files + metadata  
- `exec.exe`   : scan, filter, aggregate  

Each binary has exactly one `main()` and is built independently.

---

## Stage 1 : Ingestion

CSV input is converted into column-oriented binary storage.

One file is produced per column.

### Column file layout

```

[ColumnFileHeader]
[Dense non-null values]
[Null bitmap (1 byte per row)]

```

Properties:
- values stored contiguously
- NULLs removed from value stream
- row alignment preserved via bitmap
- columns stored independently

---

## Stage 2 : Metadata

A single metadata file is produced.

### Metadata file layout

```

[Column count]
[Column types]
[Total row count]

```

Execution does not infer schema from data files.

---

## Stage 3 : Scan and Aggregate

Execution operates on column files using fixed-size batches.

### Execution flow

```

ColumnScan -> Aggregate

```

### Batch layout

```

[values[]]
[nulls[]]
[count]

```

Aggregates (`COUNT`, `SUM`) consume batches directly.  
No row elimination at this stage.

---

## Stage 4 : Predicate Filter

Row-level selection is introduced.

### Execution flow

```

ColumnScan -> PredicateFilter -> Aggregate

```

### Predicate

```

value > threshold

```

PredicateFilter behavior:
- operates per batch
- drops rows failing predicate
- drops NULL values
- compacts output batch

### Filtered batch layout

```

[values[]]
[nulls[]]
[count ≤ input count]

```

---

## Stage 4.1 : Configurable Execution

Predicate threshold is configurable at runtime.

```

exec <meta_file> <column_file> <threshold>

```

Execution semantics unchanged.

---

## Stage 5 : Execution Pipeline

Execution orchestration is centralized in a pipeline object.

### Execution flow

```

exec_main -> Pipeline::run
Pipeline::run -> ColumnScan -> PredicateFilter -> Aggregate

```

`exec_main` performs only argument parsing and result output.

---

## Vectorized Execution Model

Execution is batch-oriented:
- fixed batch size
- tight loops over contiguous memory
- no per-row virtual dispatch
- no per-row allocation

Scalar logic is applied over column vectors.

---

## Non-Goals

This project does not include:
- SQL parsing
- query optimization
- expression trees
- joins or group-by
- concurrency or transactions
- SIMD intrinsics

---

## Summary and future of project

Column batches are scanned, filtered, and aggregated using a vectorized
execution model over column-oriented storage.

Given the limited scope of the project, it may be extended with additional execution 
features or revised in future iterations. 
