# Upgrade Notes

## Unreleased changes
- Task `options` blocks were removed; settings are now top-level properties.
- `keepUnknownFields`/`keepOtherFields` were renamed to `keepOriginalFields`.
- Unnest now always drops the exploded array field from output records.
- Zip `onError`/`onConflict` are top-level fields.
- Experimental `outputFormat: BINARY` is available for transform tasks only.

If you have existing flows, replace:

```yaml
options:
  keepOriginalFields: false
  onError: SKIP
```

with:

```yaml
keepOriginalFields: false
onError: SKIP
```
