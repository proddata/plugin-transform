# Transform Plugin Structure

This document explains how the transform tasks are organized and how the core classes connect.

## High-Level Flow

1. `io.kestra.plugin.transform.Map` receives input (`from`) and field definitions (`fields`).
2. The task resolves the input into a list of Ion structs and builds `FieldMapping` objects.
3. `DefaultTransformTaskEngine` iterates records and delegates per-record work to `DefaultRecordTransformer`.
4. `DefaultRecordTransformer` evaluates expressions, optionally casts, applies error handling, and produces a new Ion struct.
5. The task either converts Ion output to JSON-safe Java values (`records`) or stores an Ion file (`uri`) based on output mode.

## Core Classes and Responsibilities

### Task Surface

- `src/main/java/io/kestra/plugin/transform/Map.java`
  - The task entrypoint (`RunnableTask`).
  - Resolves input via `TransformTaskSupport` (typed rendering, list/map fallbacks).
  - Builds `FieldMapping` instances from `fields`, allowing shorthand values.
  - Executes the transform engine and emits records or a stored Ion file depending on `output` (AUTO/RECORDS/STORE).

- `src/main/java/io/kestra/plugin/transform/Unnest.java`
  - Explodes array paths into multiple records.
  - Uses the same input resolution and Ion streaming helpers as Map.

- `src/main/java/io/kestra/plugin/transform/Filter.java`
  - Keeps/drops records based on a boolean expression.
  - Uses the same expression engine and input resolution helpers.

- `src/main/java/io/kestra/plugin/transform/Aggregate.java`
  - Groups records and computes aggregates.
  - Uses incremental aggregation state to avoid retaining all records per group.
- `src/main/java/io/kestra/plugin/transform/Zip.java`
  - Zips multiple record streams by position and merges their fields.
  - Supports conflict handling and output modes.

- `src/main/java/io/kestra/plugin/transform/Select.java`
  - Aligns N record streams by position, optionally filters rows, and projects output fields.
  - Exposes positional input scopes via `$1`, `$2`, ... inside expressions.
  - Supports Map-style typed field definitions (optional casting via `DefaultIonCaster`).

### Expression Layer

- `src/main/java/io/kestra/plugin/transform/expression/ExpressionEngine.java`
  - Interface for expression evaluation: `evaluate(expression, record) -> IonValue`.

- `src/main/java/io/kestra/plugin/transform/expression/DefaultExpressionEngine.java`
  - Expression parser + evaluator for v1 (field access, math, boolean logic, array expansion).
  - Supports positional identifiers like `$1`, `$2`, ... for tasks that inject them into the record scope.
  - Built-in functions (`sum`, `count`, `min`, `max`, `coalesce`, etc.).
  - Produces Ion values, not strings.

- `src/main/java/io/kestra/plugin/transform/expression/ExpressionException.java`
  - Typed error surfaced by the expression engine.

### Casting Layer

- `src/main/java/io/kestra/plugin/transform/ion/IonCaster.java`
  - Interface for casting: `cast(IonValue, IonTypeName) -> IonValue`.

- `src/main/java/io/kestra/plugin/transform/ion/DefaultIonCaster.java`
  - Implements the type conversion rules for Ion values.
  - Handles nulls and type enforcement.

- `src/main/java/io/kestra/plugin/transform/ion/IonTypeName.java`
  - Enum for supported Ion types in v1.

- `src/main/java/io/kestra/plugin/transform/ion/CastException.java`
  - Typed error for cast failures.

### Transform Layer

- `src/main/java/io/kestra/plugin/transform/engine/FieldMapping.java`
  - Immutable mapping definition used internally by the engine.

- `src/main/java/io/kestra/plugin/transform/util/TransformOptions.java`
  - Options for null handling, unknown fields, and error behavior.

- `src/main/java/io/kestra/plugin/transform/engine/RecordTransformer.java`
  - Interface: per-record transform contract.

- `src/main/java/io/kestra/plugin/transform/engine/DefaultRecordTransformer.java`
  - Core record transform logic:
    - Evaluate expressions using the expression engine.
    - Cast if a type is provided; otherwise pass through the evaluated value.
    - Apply `keepOriginalFields`, `dropNulls`, and `onError` rules.
    - Collect field-level errors.

- `src/main/java/io/kestra/plugin/transform/util/TransformException.java`
  - Typed error for transform failures.

### Execution Layer

- `src/main/java/io/kestra/plugin/transform/engine/TransformTaskEngine.java`
  - Interface: transforms a list of records and returns a `TransformResult`.

- `src/main/java/io/kestra/plugin/transform/engine/DefaultTransformTaskEngine.java`
  - Iterates input records and aggregates `TransformStats`.
  - Applies skip/drop/error logic per record.

- `src/main/java/io/kestra/plugin/transform/engine/TransformResult.java`
  - Output model: transformed records + internal stats.

- `src/main/java/io/kestra/plugin/transform/engine/TransformStats.java`
  - Metrics: processed, failed, dropped, fieldErrors.

### Utilities

- `src/main/java/io/kestra/plugin/transform/ion/IonValueUtils.java`
  - Ion system helper, conversion utilities, cloning, and Ion-to-Java serialization.
  - Used across expression, casting, and output conversion.

- `src/main/java/io/kestra/plugin/transform/util/TransformTaskSupport.java`
  - Shared helpers for input resolution, Ion loading, and output stream setup.

### Plugin Metadata

- `src/main/java/io/kestra/plugin/transform/package-info.java`
  - `@PluginSubGroup` for UI grouping.

- `src/main/resources/metadata/index.yaml`
  - Plugin metadata for Kestra discovery.

## Connection Diagram (Conceptual)

```
Map/Unnest/Filter/Aggregate/Zip/Select
  -> DefaultTransformTaskEngine (Map only)
  -> DefaultRecordTransformer (Map only)
  -> DefaultExpressionEngine
  -> DefaultIonCaster (optional, only when type provided)
  -> IonValueUtils (nulls, cloning, conversions)
  -> TransformTaskSupport (input/output helpers)
```

## Notes on Output

- Task output uses JSON-safe Java values to avoid serializing Ion internals.
- Processing counters are emitted as task metrics instead of output stats.
- `output: AUTO` stores results when `from` resolves to a storage URI; otherwise it returns `records`.
- `output: STORE` emits `uri` only; `output: RECORDS` emits `records` only.
- If `type` is omitted, the expression result is returned as-is.
- Shorthand field definitions (e.g., `customer_id: user.id`) are supported.
