# Kestra Transform Plugin

Typed, streaming-friendly transform tasks for Kestra using Amazon Ion.

## Tasks
- `io.kestra.plugin.transform.Map`: project/rename/cast fields
- `io.kestra.plugin.transform.Unnest`: explode array fields into rows
- `io.kestra.plugin.transform.Filter`: keep/drop records by boolean expression
- `io.kestra.plugin.transform.Aggregate`: group-by and typed aggregates
- `io.kestra.plugin.transform.Zip`: merge multiple record streams by position
- `io.kestra.plugin.transform.Select`: align N inputs by position + optional filter + projection

## Input and output
Most tasks accept:
- `from`: in-memory records (map/list/Ion) or a storage URI (`kestra://...`)

Multi-input tasks:
- `Zip`: `inputs` (list)
- `Select`: `inputs` (list)

Output mode (`output`) supports:
- `AUTO` (default): STORE if `from` is a storage URI, else RECORDS
- `RECORDS`: emit `outputs.records`
- `STORE`: write newline-delimited Ion to internal storage and emit `outputs.uri`

Experimental output format (`outputFormat`):
- `TEXT` (default): newline-delimited Ion text
- `BINARY`: Ion binary output (only readable by transform tasks; use TEXT as final step)

## Common properties
- `from`: input records or storage URI (all tasks except Zip/Select)
- `output`: `AUTO | RECORDS | STORE`
- `onError`: error handling; varies by task (see below)

## Task reference

Map (`io.kestra.plugin.transform.Map`)
- `from`, `fields`
- `keepOriginalFields` (default `false`): keep input fields not mapped by target name
- `dropNulls` (default `true`): drop null fields from output
- `onError` (default `FAIL`): `FAIL | SKIP | NULL`
- `outputFormat` (default `TEXT`): experimental `TEXT | BINARY` (binary only readable by transform tasks; use TEXT as final step)
- Outputs: `records` or `uri`

Unnest (`io.kestra.plugin.transform.Unnest`)
- `from`, `path`, `as`
- `keepOriginalFields` (default `true`): keep original fields except the exploded array field
- `onError` (default `FAIL`): `FAIL | SKIP | NULL`
- `outputFormat` (default `TEXT`): experimental `TEXT | BINARY` (binary only readable by transform tasks; use TEXT as final step)
- Outputs: `records` or `uri`

Filter (`io.kestra.plugin.transform.Filter`)
- `from`, `where`
- `onError` (default `FAIL`): `FAIL | SKIP | KEEP`
- `outputFormat` (default `TEXT`): experimental `TEXT | BINARY` (binary only readable by transform tasks; use TEXT as final step)
- Outputs: `records` or `uri`

Aggregate (`io.kestra.plugin.transform.Aggregate`)
- `from`, `groupBy`, `aggregates`
- `onError` (default `FAIL`): `FAIL | SKIP | NULL`
- `outputFormat` (default `TEXT`): experimental `TEXT | BINARY` (binary only readable by transform tasks; use TEXT as final step)
- Outputs: `records` or `uri`

Zip (`io.kestra.plugin.transform.Zip`)
- `inputs` (list of inputs)
- `onError` (default `FAIL`): `FAIL | SKIP`
- `onConflict` (default `FAIL`): `FAIL | LEFT | RIGHT`
- `outputFormat` (default `TEXT`): experimental `TEXT | BINARY` (binary only readable by transform tasks; use TEXT as final step)
- `output`: `AUTO` stores if any input is a storage URI
- Outputs: `records` or `uri`

Select (`io.kestra.plugin.transform.Select`)
- `inputs` (list of inputs), optional `where`, optional `fields`
- Positional references: `$1.field`, `$2.field`, ... (unqualified fields resolve from merged row)
- `fields` supports Map-style definitions: shorthand `field: expr` or full `{ expr, type, optional }`
- `keepOriginalFields` (default `false`): when `fields` is provided, include merged row fields too
- `dropNulls` (default `true`): drop null fields from output
- `onLengthMismatch` (default `FAIL`): `FAIL | SKIP`
- `onError` (default `FAIL`): `FAIL | SKIP | KEEP`
- `outputFormat` (default `TEXT`): experimental `TEXT | BINARY` (binary only readable by transform tasks; use TEXT as final step)
- `output`: `AUTO` stores if any input is a storage URI
- Outputs: `records` or `uri`

## Expression language (v1)
- Field access: `user.id`
- Nested: `user.address.city`
- Arrays: `items[].price`
- Comparisons: `> < == != >= <=`
- Boolean: `&& || !`
- Functions: `sum`, `count`, `min`, `max`, `avg`, `first`, `last`, `coalesce`, `concat`, `toInt`, `toDecimal`, `toString`, `toBoolean`, `parseTimestamp`

## Type casting
- `type` is optional in Map/Aggregate definitions.
- If `type` is omitted, the evaluated Ion value is passed through.
- `TIMESTAMP` supports Ion timestamps and ISO-8601 strings (`parseTimestamp` can convert strings).

## Examples
Map with casting and error handling:
```yaml
- id: normalize
  type: io.kestra.plugin.transform.Map
  from: "{{ outputs.fetch.records }}"

  fields:
    customer_id:
      expr: user.id
      type: STRING
    created_at:
      expr: createdAt
      type: TIMESTAMP
    total:
      expr: sum(items[].price)
      type: DECIMAL

  keepOriginalFields: false
  dropNulls: true
  onError: SKIP
```
Note: `keepOriginalFields` keeps input fields not mapped by name; mapping `a_new: a` still keeps the original `a`.

Unnest + Filter + Map:
```yaml
- id: explode_items
  type: io.kestra.plugin.transform.Unnest
  from: "{{ outputs.fetch.records }}"
  path: items[]
  as: item

- id: expensive_items
  type: io.kestra.plugin.transform.Filter
  from: "{{ outputs.explode_items.records }}"
  where: item.price > 10

- id: project
  type: io.kestra.plugin.transform.Map
  from: "{{ outputs.expensive_items.records }}"
  fields:
    customer_id: customer_id
    sku: item.sku
    price:
      expr: item.price
      type: DECIMAL
```
Note: Unnest drops the exploded array field (`items` here) while keeping other fields.

Aggregate totals:
```yaml
- id: totals
  type: io.kestra.plugin.transform.Aggregate
  from: "{{ outputs.normalize.records }}"
  groupBy:
    - customer_id
    - country
  aggregates:
    order_count:
      expr: count()
      type: INT
    total_spent:
      expr: sum(total_spent)
      type: DECIMAL
    last_order_at:
      expr: max(created_at)
      type: TIMESTAMP
  onError: FAIL
```

## Examples index
- `examples/api_to_typed_records.yml`: normalize API output into typed fields
- `examples/http_download_transform.yml`: download products, unnest, map, and store
- `examples/dummyjson_products_flow.yml`: unnest products, filter, map
- `examples/dummyjson_carts_flow.yml`: compute max product total per cart and filter
- `examples/dummyjson_users_flow.yml`: unnest users, map, filter
- `examples/aggregate_totals.yml`: group and aggregate totals
- `examples/zip_basic.yml`: zip two record streams by position

More flows live in `examples/`.

## Migration notes
See `docs/UPGRADE.md` for breaking changes (options flattening, renamed fields).

## Development
Prerequisites:
- Java 21
- Docker

Run tests:
```sh
./gradlew test
```

Run Kestra locally with the plugin:
```sh
./gradlew shadowJar && docker build -t kestra-custom . && docker run --rm -p 8080:8080 kestra-custom server local
```

## Benchmarks
Opt-in benchmarks live in `src/test/java/io/kestra/plugin/transform/BenchTest.java`.

Examples:
```sh
./gradlew test --tests io.kestra.plugin.transform.BenchTest -Dbench=true -Dbench.records=10000,100000 -Dbench.format=text
./gradlew test --tests io.kestra.plugin.transform.BenchTest -Dbench=true -Dbench.records=10000,100000 -Dbench.format=binary
```

Reports are written to `build/bench/report.md`.

## Documentation
Kestra docs: https://kestra.io/docs  
Plugin developer guide: https://kestra.io/docs/plugin-developer-guide
