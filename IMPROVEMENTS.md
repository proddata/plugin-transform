# Potential Improvements

## IO and storage
- Support binary Ion as an opt-in output mode (STORE_BINARY) with compatibility notes.
- Optional compression (gzip/zstd) on STORE outputs with explicit metadata.
- Buffered streaming for input + output across all tasks (already improved, can be formalized).

## Aggregation scalability
- Spill-to-disk group state for high-cardinality aggregates.
- Optional two-pass or partitioned aggregation to bound memory.
- Incremental aggregation already reduces memory; add limits/metrics per group.

## Error handling and diagnostics
- Add structured error details (field, index, reason) to logs or metrics.
- More precise validation messages (e.g., path parsing errors in Unnest/Filter).
- Optional "maxErrors" to stop early but keep partial output.

## Benchmarks
- Separate benchmark module or Gradle task to avoid accidental test runs.
- Include NDJSON baseline for comparison.
- Add a summary comparator report across formats/compression.

## Code organization
- Additional shared utilities for stream record iteration and output writing.
- Consolidate "onError" semantics where possible across tasks.
- Extract common path evaluation helpers into the expression layer.
