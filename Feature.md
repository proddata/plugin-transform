### Problem Statement

Kestra is primarily a configuration-driven (YAML) orchestration platform. Tasks can range from simple API calls to complex data processing, and they usually produce outputs in one of two ways:

- **Output properties** (small, structured results)
- **Internal storage** for larger datasets (e.g. >1 MB), stored by default in **Amazon Ion**, a **typed** data format

When tasks read data from sources like databases, Kestra preserves column types in Ion. This is powerful because those types can later be reused when writing the data back to another system.

A core usage pattern in Kestra is chaining tasks and passing data from one task to the next. However, in practice the output of one task often cannot be consumed directly by the next — it needs lightweight transformation, such as:

- renaming fields  
- combining multiple fields  
- casting types  
- reshaping objects  

Today, this is fragmented:

- `OutputValues` can reshape **output properties**, but cannot operate on files stored in internal storage
- **Pebble expressions** can do some JSON-level manipulation, but:
  - they break the otherwise declarative YAML model
  - they do not work on typed Ion files
  - they are awkward for non-trivial transformations
- **Script tasks (JS, Python, etc.)** can do everything, but:
  - they are imperative
  - they discard type information
  - they break the configuration-driven nature of Kestra

As a result, there is no configuration-based way to perform simple, typed transformations across both:
- output properties
- and internal storage (Ion) files

This forces users into either brittle Pebble templates or full scripting for tasks that should be simple, declarative data shaping.

### Proposed Solution

Introduce a small, typed record-level transform engine as a Kestra plugin with a focused set of transform tasks that share a common I/O model.

#### Common behavior (current implementation)

All transform tasks:
- Accept in-memory records (map/list) or a storage URI (`kestra://...`) pointing to an Ion file.
- Support `output: AUTO | RECORDS | STORE`:
  - `AUTO` (default): if the input is a storage URI, behaves like `STORE`; otherwise behaves like `RECORDS`.
  - `RECORDS`: emit `outputs.records` as JSON-safe values (maps/lists/primitives).
  - `STORE`: write newline-delimited Ion to internal storage and emit `outputs.uri`.
- Support experimental `outputFormat: TEXT | BINARY`:
  - `TEXT` is newline-delimited Ion text.
  - `BINARY` is Ion binary (only readable by transform tasks; use `TEXT` as the final step).
- Emit task metrics (counters) such as `processed`, `failed`, `dropped` (and task-specific counters like `passed` or `groups`).

#### `io.kestra.plugin.transform.Map`

The Map task is a declarative **"select + rename + derive"** operator: it takes records and emits new records containing the declared fields (optionally keeping unknown fields).

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

- **Original fields**
  - `keepOriginalFields: true` copies input fields not declared in `fields` into the output record.
  - `keepOriginalFields: false` outputs only the declared fields.
  - If you map `a_new: a`, the original `a` is still kept because only target field names are considered mapped.

- **Error handling**
  - `onError: FAIL | SKIP | NULL`
    - FAIL: fail the task
    - SKIP: drop the entire record
    - NULL: set the failing field to null (unless `dropNulls` removes it)

**What it does not try to do (by design)**

- Cross-record operations (joins, group-by, windowing)
- Side effects or parallelism
- Complex data construction (e.g., struct/list literals) beyond selecting/deriving scalars and traversing existing arrays

#### `io.kestra.plugin.transform.Unnest`

The Unnest task explodes an array field into multiple records (one per element).

- **Inputs**
  - `from`: input records or a storage URI.
- **Unnesting**
  - `path`: array path to explode (e.g., `items[]`).
  - `as`: field name that receives the element value (e.g., `item`).
  - `keepOriginalFields: true` (default) keeps the original record fields except the unnested array field.
- **Error handling**
  - `onError: FAIL | SKIP | NULL`

#### `io.kestra.plugin.transform.Filter`

The Filter task keeps or drops records based on a boolean expression evaluated per record.

- **Inputs**
  - `from`: input records or a storage URI.
- **Filtering**
  - `where`: boolean expression (e.g., `is_active && total_spent > 100`).
- **Error handling**
  - `onError: FAIL | SKIP | KEEP`
    - KEEP means the record is kept when evaluation fails.

#### `io.kestra.plugin.transform.Aggregate`

The Aggregate task groups records by one or more keys and computes typed aggregates.

- **Inputs**
  - `from`: input records or a storage URI.
- **Grouping**
  - `groupBy`: list of field paths used as the grouping key.
- **Aggregates**
  - `aggregates`: map of output field name -> `{ expr, type, optional }` (type optional).
- **Error handling**
  - `onError: FAIL | SKIP | NULL`

#### `io.kestra.plugin.transform.Zip`

The Zip task merges multiple record streams by position (record i with record i).

- **Inputs**
  - `inputs`: list of inputs (each can resolve to lists/maps/Ion values or a storage URI).
  - Inputs must have the same length.

- **Conflict handling**
  - `onConflict: FAIL | LEFT | RIGHT` controls how field name collisions are resolved.

- **Error handling**
  - `onError: FAIL | SKIP` controls whether to drop the record pair or fail the task.

#### `io.kestra.plugin.transform.Select`

The Select task aligns N record streams by position and then optionally filters and projects each merged row.

- **Inputs**
  - `inputs`: list of inputs (each can be in-memory records or a storage URI).
  - Later inputs override earlier ones when fields collide.
  - Positional references: `$1`, `$2`, ... (e.g., `$2.name`).

- **Filtering**
  - `where` (optional): boolean expression evaluated on the merged row.

- **Projection**
  - `fields` (optional): output field name -> field definition.
  - Supports Map-style field definitions: shorthand `field: expr` or full `{ expr, type, optional }` (type optional).
  - If `fields` is omitted, output defaults to the merged row.
  - `keepOriginalFields` controls whether the merged row is kept when projecting.

- **Error handling**
  - `onLengthMismatch: FAIL | SKIP`
  - `onError: FAIL | SKIP | KEEP` (KEEP emits the original merged row)
