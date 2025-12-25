### Problem Statement

Kestra is primarily a **configuration-driven (YAML) orchestration platform**. Tasks can range from simple API calls to complex data processing, and they usually produce outputs in one of two ways:

- **Output properties** (small, structured results)
- **Internal storage** for larger datasets (e.g. >1 MB), stored by default in **Amazon Ion**, a **typed** data format

When tasks read data from sources like databases, Kestra preserves column types in Ion. This is powerful because those types can later be reused when writing the data back to another system.

A core usage pattern in Kestra is **chaining tasks** and passing data from one task to the next. However, in practice the output of one task often cannot be consumed directly by the next — it needs **lightweight transformation**, such as:

- renaming fields  
- combining multiple fields  
- casting types  
- reshaping objects  

Today, this is fragmented:

- `OutputValues` can reshape **output properties**, but **cannot operate on files** stored in internal storage
- **Pebble expressions** can do some JSON-level manipulation, but:
  - they break the otherwise declarative YAML model
  - they do not work on typed Ion files
  - they are awkward for non-trivial transformations
- **Script tasks (JS, Python, etc.)** can do everything, but:
  - they are imperative
  - they discard type information
  - they break the configuration-driven nature of Kestra

As a result, there is **no configuration-based way** to perform **simple, typed transformations** across both:
- output properties
- and internal storage (Ion) files

This forces users into either brittle Pebble templates or full scripting for tasks that should be simple, declarative data shaping.

### Proposed Solution

Introduce a small, typed **record-level transform engine** as a Kestra plugin, starting with a single MVP task:

#### `io.kestra.plugin.transform.Map` (current implementation)

The Map task is a **declarative “select + rename + derive”** operator: it takes a list of records and emits a new list of records containing exactly the declared fields (optionally keeping unknown fields).

**What it can do today**

- **Input shapes**
  - `from` can resolve to a `List<Map<...>>` (typical when chaining task outputs), a single `Map`, or a list of Ion structs (when provided as objects).
  - The task uses Kestra’s `renderTyped(...)`/`asList(...)`/`asMap(...)` to avoid treating `{{ ... }}` as a plain string.
  - `from` can also be a **storage URI** (string/URI) pointing to an **Ion file** in Kestra internal storage; the task will load the Ion values and treat them as records.

- **Field mapping**
  - Each entry under `fields` defines one output field.
  - Supported forms:
    - Full form:
      - `customer_id: { expr: user.id, type: STRING, optional: false }`
    - Shorthand (expr-only):
      - `customer_id: user.id`

- **Expressions (v1 engine)**
  - Field access: `user.id`, `user.name.first`
  - Nested traversal with array expansion: `items[].price`
  - Arithmetic: `price * quantity`, `sum(items[].price)`
  - Boolean logic: `active && paid`
  - Literals: `true`, `false`, `null`, numeric and string literals
  - Built-ins: `toInt`, `toDecimal`, `toString`, `toBoolean`, `parseTimestamp`, `sum`, `count`, `min`, `max`, `coalesce`, `concat`

- **Typing / casting**
  - `type` is **optional**:
    - If `type` is provided, the result is cast via the Ion caster (e.g., `STRING`, `DECIMAL`, `TIMESTAMP`).
    - If `type` is omitted, the evaluated value is **passed through** (inferred).

- **Null handling**
  - `dropNulls: true` removes null-valued fields from the output record.

- **Unknown fields**
  - `keepUnknownFields: true` copies input fields not declared in `fields` into the output record.
  - `keepUnknownFields: false` outputs only the declared fields.

- **Error handling**
  - `onError: FAIL | SKIP | NULL`
    - FAIL: fail the task
    - SKIP: drop the entire record
    - NULL: set the failing field to null (unless `dropNulls` removes it)
- Processing counters are emitted as task metrics (processed/failed/dropped).

**What it emits today**

- Output supports three modes:
  - `output: AUTO` (default): if `from` resolves to a storage URI, behaves like `STORE`; otherwise behaves like `RECORDS`.
  - `output: RECORDS`: `outputs.records` is emitted as **JSON-safe values** (maps/lists/primitives), to avoid serializing Ion’s internal object graphs in Kestra outputs.
  - `output: STORE`: writes the transformed records as an **Ion file** to Kestra internal storage and returns `outputs.uri`.
  - `DECIMAL` becomes `BigDecimal`
  - `TIMESTAMP` becomes an ISO-8601 string
- Tasks emit metrics (processed/failed/dropped/passed/groups) instead of stats outputs.

**What it does not try to do (by design, for MVP)**

- Cross-record operations (joins, group-by, windowing)
- Side effects or parallelism
- Complex data construction (e.g., struct/list literals) beyond selecting/deriving scalars and traversing existing arrays

### Future Extensions

Once `Map` proves the typed, UI-friendly, statically validatable approach, the next tasks can build on the same core engine:

#### Filter

A record-level predicate operator:

- Keeps or drops records based on a boolean expression evaluated per record.
- Reuses the same expression engine as `Map`.
- Suggested API:
  - `where: <expression>`
  - `onError: FAIL | SKIP | KEEP` (or align with `Map`’s modes)
- Output preserves the original record shape (or optionally supports projecting through `Map`).

#### Union

A multi-input concatenation / schema alignment operator:

- Combines multiple record streams (lists) into one.
- Optional schema alignment:
  - strict: require identical field sets and types
  - permissive: union fields, fill missing with null
- Useful to merge partitions, branches, or multiple APIs into a single normalized stream.

#### Aggregate

A grouped aggregation operator (bounded scope, still declarative):

- Groups records by one or more keys, then computes derived aggregate fields.
- Aggregations would reuse built-ins like `sum`, `count`, `min`, `max`, and potentially new ones.
- Suggested API:
  - `groupBy: [fieldA, fieldB]`
  - `aggregates: { total: { expr: sum(items[].price), type: DECIMAL } }`
- Explicitly a separate task from `Map` to keep the MVP simple and the UI model clear.
