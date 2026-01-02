# Expression language cheat sheet (v1)

The transform tasks use a small expression language for field access, filtering, and projections.

## Field access

- Dot access: `user.id`, `user.address.city`
- Array expansion: `items[].price` returns a list of prices
- Bracket access (for keys with spaces/special chars): `user["first name"]`

In `Select`, each input is also available positionally:

- `$1`, `$2`, ... (e.g. `$2.name`, `$1["order id"]`)

Unqualified fields resolve against the merged/default scope (last input wins on conflicts).

## Literals

- Numbers: `1`, `12.5`
- Strings: `"hello"` (supports escapes like `\"`, `\\`, `\n`, `\t`)
- Booleans: `true`, `false`
- Null: `null`

## Operators

- Arithmetic: `+ - * /`
- Comparison: `> < >= <= == !=`
- Boolean: `&& || !`

## Common patterns

- Filter records: `active && total_spent > 100`
- Sum values from arrays: `sum(items[].price)`
- Count items: `count(items[])`
- Coalesce: `coalesce(user.email, user.username)`
- String concat: `concat(first_name, " ", last_name)`

## Built-in functions

- Type helpers: `toInt`, `toDecimal`, `toString`, `toBoolean`, `parseTimestamp`
- Aggregation over lists: `sum`, `count`, `min`, `max`
- Other: `coalesce`, `concat`

## Notes

- `items[].price` produces a list; functions like `sum`/`min`/`max` expect a list argument.
- Bracket syntax uses double quotes inside the expression: `record["field name"]`. In YAML you’ll usually wrap the whole expression in single quotes.
- `&&` and `||` are short-circuiting; if the left side is enough to decide, the right side isn’t evaluated.
- Boolean nulls propagate: `null && true` and `null || false` both return `null`.
