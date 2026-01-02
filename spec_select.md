Kestra Plugin Task Specification

io.kestra.plugin.transform.Select

⸻

1. Purpose

Select is a multi-input, row-wise transformation task that:
	1.	aligns multiple datasets positionally
	2.	optionally filters rows
	3.	projects them into new records

It combines the functionality of:
	•	Zip
	•	Filter
	•	Map

into a single streaming operator.

It does not perform key-based joins.
It operates strictly by row position.

⸻

2. Mental model

Given:

inputs = [A, B, C]

For each index i, the task builds a row context:

$1 = A[i]
$2 = B[i]
$3 = C[i]

merged = { ...$1, ...$2, ...$3 }   # later inputs override earlier ones

Then:

if where(merged, $1, $2, $3) == true:
    emit project(merged, $1, $2, $3)

This is evaluated streaming row by row.

⸻

3. Positional addressing

Each input is exposed as:

Reference	Meaning
$1	row from first input
$2	row from second input
$n	row from nth input

Additionally, all fields are accessible from a merged default scope:

merged = { ...$1, ...$2, ...$3, ...$n }

Later inputs override earlier ones.

Inside expressions:
	•	fieldA resolves to merged.fieldA
	•	$2.fieldA resolves to the field from input 2

⸻

4. Properties

inputs:                      # required
  - <records or storage URI>
  - <records or storage URI>
  - ...

where: <expression>          # optional filter expression

fields:                      # projection mapping (optional)
  <targetField>: <expression>                     # shorthand
  <targetField>: { expr: <expr>, type: <TYPE> }   # typed
  <targetField>: { expr: <expr>, optional: true } # allow missing/null
  ...

keepOriginalFields: false | true   # default false
dropNulls: true | false            # default true

onLengthMismatch: FAIL | SKIP      # default FAIL
onError: FAIL | SKIP | KEEP        # default FAIL

outputFormat: TEXT | BINARY        # default TEXT
output: AUTO | RECORDS | STORE     # default AUTO (URI is accepted as an alias of STORE)


⸻

5. Behavior

5.1 Input alignment

All inputs are iterated in parallel by index.

Case	Behavior
Same length	normal
Different length + FAIL	task fails
Different length + SKIP	rows missing any input are skipped


⸻

5.2 Filtering (where)

The where expression is evaluated per row.

If:
	•	true → row is processed
	•	false → row is skipped
	•	error → handled via onError

⸻

5.3 Projection (fields)

If fields is provided:
	•	Each output row is built from these expressions
	•	type is optional; when provided the value is cast like Map
	•	optional=false (default) means null/missing is an error ("Missing required field: <field>")

If fields is omitted:
	•	Output defaults to merged (flattened last-wins object)

If keepOriginalFields=true:
	•	projected fields are added on top of merged

If dropNulls=true:
	•	null fields are removed from output

⸻

5.4 Error handling

Mode	Behavior
FAIL	task fails on any expression/cast error
SKIP	row is dropped
KEEP	row is emitted using original merged row


⸻

6. Example

Inputs

orders.json

{"order_id":1,"customer_id":"c1","amount":120}
{"order_id":2,"customer_id":"c2","amount":80}

customers.json

{"id":"c1","name":"Anna"}
{"id":"c2","name":"Ben"}

scores.json

{"score":0.91}
{"score":0.72}


⸻

Task

id: enrich_orders
type: io.kestra.plugin.transform.Select

inputs:
  - s3://data/orders.json
  - s3://data/customers.json
  - s3://data/scores.json

where: amount > 100 && $3.score > 0.8

fields:
  orderId: order_id
  customer: $2.name
  amount: $1.amount
  score: $3.score

keepOriginalFields: false


⸻

Output

{"orderId":1,"customer":"Anna","amount":120,"score":0.91}


⸻

7. Non-goals

Select does NOT:
	•	perform key-based joins
	•	reorder rows
	•	aggregate
	•	group
	•	sort

It is strictly:

positional alignment + filtering + projection
