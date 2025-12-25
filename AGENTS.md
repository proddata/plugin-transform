# AGENTS.md

## Project overview
- Repository: Kestra Transform plugin (`io.kestra.plugin.transform`).
- Main task: `io.kestra.plugin.transform.Map`.
- Language/build: Java 21 with Gradle.

## Key locations
- Task implementation: `src/main/java/io/kestra/plugin/transform/Map.java`.
- Core transform engine: `src/main/java/io/kestra/plugin/transform/engine/DefaultTransformTaskEngine.java`.
- Expression engine: `src/main/java/io/kestra/plugin/transform/expression/DefaultExpressionEngine.java`.
- Ion utilities: `src/main/java/io/kestra/plugin/transform/ion/IonValueUtils.java`.
- Tests: `src/test/java/io/kestra/plugin/transform/MapTest.java`.
- Docs: `README.md`, `docs/TRANSFORM_STRUCTURE.md`, `Feature.md`.
- Plugin metadata: `metadata/index.yaml`.
- Example flows: `examples/` (YAML flows used for docs and manual testing).

## Build and test
- Run tests: `./gradlew test`.
- Build shadow jar: `./gradlew shadowJar`.
- Local dev (from README): `./gradlew shadowJar && docker build -t kestra-custom . && docker run --rm -p 8080:8080 kestra-custom server local`.
- Benchmarks (opt-in): `./gradlew test -Dbench=true --tests io.kestra.plugin.transform.BenchTest` (report: `build/bench/report.txt`).

## Map task behavior (current)
- Input `from` can be a list/map, a single map, Ion structs/lists, or a storage URI (string/URI).
- Output modes:
  - `AUTO` (default): if `from` resolves to a storage URI, store Ion and emit `outputs.uri`; otherwise emit `outputs.records`.
  - `RECORDS`: emit JSON-safe values via `outputs.records`.
  - `STORE`: write Ion to internal storage and emit `outputs.uri`.
- Output stats are always emitted in `outputs.stats`.

## Notes for contributors
- Prefer ASCII in edits unless the file already uses Unicode.
- Use `apply_patch` for single-file edits when practical.
- Do not revert unrelated local changes.
- Tests emit some deprecated API warnings (AspectJ); these are expected.
