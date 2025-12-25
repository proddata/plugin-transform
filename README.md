# Kestra Transform Plugin

Typed, streaming-friendly transform tasks for Kestra using Amazon Ion.

## Tasks
- `io.kestra.plugin.transform.Map`: project/rename/cast fields
- `io.kestra.plugin.transform.Unnest`: explode array fields into rows
- `io.kestra.plugin.transform.Filter`: keep/drop records by boolean expression
- `io.kestra.plugin.transform.Aggregate`: group-by and typed aggregates
- `io.kestra.plugin.transform.Zip`: merge two record streams by position

## Input and output
All tasks accept:
- `from`: in-memory records (map/list/Ion) or a storage URI (`kestra://...`)

Output mode (`output`) supports:
- `AUTO` (default): STORE if `from` is a storage URI, else RECORDS
- `RECORDS`: emit `outputs.records`
- `STORE`: write newline-delimited Ion to internal storage and emit `outputs.uri`

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

More flows live in `examples/`.

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
