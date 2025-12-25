package io.kestra.plugin.transform;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.transform.engine.DefaultRecordTransformer;
import io.kestra.plugin.transform.engine.DefaultTransformTaskEngine;
import io.kestra.plugin.transform.engine.FieldMapping;
import io.kestra.plugin.transform.engine.TransformResult;
import io.kestra.plugin.transform.engine.TransformStats;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.ion.DefaultIonCaster;
import io.kestra.plugin.transform.ion.IonTypeName;
import io.kestra.plugin.transform.ion.IonValueUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Typed record mapping",
    description = "Declaratively map, cast, and derive Ion fields without scripts."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Normalize records",
            code = {
                "from: \"{{ outputs.fetch.records }}\"",
                "fields:",
                "  customer_id:",
                "    expr: user.id",
                "    type: STRING",
                "  created_at:",
                "    expr: createdAt",
                "    type: TIMESTAMP",
                "  total:",
                "    expr: sum(items[].price)",
                "    type: DECIMAL",
                "options:",
                "  dropNulls: true",
                "  onError: SKIP"
            }
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Map DuckDB stored results",
            full = true,
            code = """
                id: map_duckdb_stored
                namespace: company.team

                tasks:
                  - id: query1
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    sql: SELECT now() as "ts";
                    fetchType: STORE

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.query1.uri }}"
                    output: RECORDS
                    fields:
                      ts:
                        expr: ts
                        type: TIMESTAMP
                """
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Map direct output values",
            full = true,
            code = """
                id: map_direct_outputs
                namespace: company.team

                tasks:
                  - id: query1
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    sql: SELECT 1 as "value" UNION ALL SELECT 2 as "value";
                    fetchType: FETCH

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.query1.records }}"
                    fields:
                      value:
                        expr: value
                        type: INT
                """
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Download JSON and transform",
            full = true,
            code = """
                id: map_http_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://dummyjson.com/products

                  - id: map
                    type: io.kestra.plugin.transform.Map
                    from: "{{ outputs.download.uri }}"
                    output: RECORDS
                    fields:
                      first_title:
                        expr: products[0].title
                        type: STRING
                """
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE)
    }
)
public class Map extends Task implements RunnableTask<Map.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform, or a storage URI pointing to an Ion file."
    )
    private Property<Object> from;

    @Schema(
        title = "Field mappings",
        description = "Target fields with expressions and types."
    )
    private java.util.Map<String, FieldDefinition> fields;

    @Schema(
        title = "Transform options",
        description = "Error and null handling for the transform."
    )
    @Builder.Default
    private Options options = new Options();

    @Schema(
        title = "Output mode",
        description = "AUTO stores to internal storage when the input is a storage URI; otherwise it returns records."
    )
    @Builder.Default
    private OutputMode output = OutputMode.AUTO;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ResolvedInput resolvedInput = resolveInput(runContext);

        List<FieldMapping> mappings = new ArrayList<>();
        if (fields != null) {
            for (Entry<String, FieldDefinition> entry : fields.entrySet()) {
                FieldDefinition definition = entry.getValue();
                if (definition == null) {
                    throw new TransformException("Field definition is required for '" + entry.getKey() + "'");
                }
                mappings.add(new FieldMapping(
                    entry.getKey(),
                    Objects.requireNonNull(definition.expr, "expr is required"),
                    definition.type,
                    definition.optional
                ));
            }
        }

        TransformOptions transformOptions = options.toOptions();
        DefaultRecordTransformer transformer = new DefaultRecordTransformer(
            mappings,
            new DefaultExpressionEngine(),
            new DefaultIonCaster(),
            transformOptions
        );
        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        if (resolvedInput.fromStorage() && effectiveOutput == OutputMode.STORE) {
            StreamResult streamResult = transformStreamToStorage(
                runContext,
                resolvedInput.storageUri(),
                transformer
            );
            runContext.metric(Counter.of("processed", streamResult.stats().processed()))
                .metric(Counter.of("failed", streamResult.stats().failed()))
                .metric(Counter.of("dropped", streamResult.stats().dropped()));
            return Output.builder()
                .uri(streamResult.uri().toString())
                .build();
        }

        List<IonStruct> records = normalizeRecords(resolveInMemory(runContext, resolvedInput));
        DefaultTransformTaskEngine engine = new DefaultTransformTaskEngine(transformer);
        TransformResult result = engine.execute(records);

        runContext.metric(Counter.of("processed", result.stats().processed()))
            .metric(Counter.of("failed", result.stats().failed()))
            .metric(Counter.of("dropped", result.stats().dropped()));

        if (effectiveOutput == OutputMode.STORE) {
            URI storedUri = storeRecords(runContext, result.records());
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<Object> renderedRecords = new ArrayList<>();
        for (IonStruct record : result.records()) {
            renderedRecords.add(IonValueUtils.toJavaValue(record));
        }

        return Output.builder()
            .records(renderedRecords)
            .build();
    }

    private Object resolveInMemory(RunContext runContext, ResolvedInput resolvedInput) throws TransformException {
        if (!resolvedInput.fromStorage()) {
            return resolvedInput.value();
        }
        return loadIonFromStorage(runContext, resolvedInput.storageUri());
    }

    private StreamResult transformStreamToStorage(RunContext runContext,
                                                  URI uri,
                                                  DefaultRecordTransformer transformer) throws TransformException {
        String name = "transform-" + UUID.randomUUID() + ".ion";
        java.util.Map<String, String> fieldErrors = new java.util.HashMap<>();
        int processed = 0;
        int failed = 0;
        int dropped = 0;
        InputStream inputStream;
        try {
            inputStream = runContext.storage().getFile(uri);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }

        try (InputStream stream = inputStream) {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream fileStream = java.nio.file.Files.newOutputStream(outputPath);
                 OutputStream outputStream = new java.io.BufferedOutputStream(fileStream, FileSerde.BUFFER_SIZE);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                Iterator<IonValue> iterator = IonValueUtils.system().iterate(stream);
                int index = 0;
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            StreamCounters counters = processStreamRecord(element, transformer, writer, outputStream, fieldErrors, index);
                            processed++;
                            if (counters.failed) {
                                failed++;
                            }
                            if (counters.dropped) {
                                dropped++;
                            }
                            index = counters.nextIndex;
                        }
                    } else {
                        StreamCounters counters = processStreamRecord(value, transformer, writer, outputStream, fieldErrors, index);
                        processed++;
                        if (counters.failed) {
                            failed++;
                        }
                        if (counters.dropped) {
                            dropped++;
                        }
                        index = counters.nextIndex;
                    }
                }
                writer.finish();
            }
            URI storedUri = runContext.storage().putFile(outputPath.toFile(), name);
            return new StreamResult(
                new TransformStats(processed, failed, dropped, fieldErrors),
                storedUri
            );
        } catch (IOException e) {
            throw new TransformException("Unable to store transformed records", e);
        }
    }

    private StreamCounters processStreamRecord(IonValue value,
                                               DefaultRecordTransformer transformer,
                                               IonWriter writer,
                                               OutputStream outputStream,
                                               java.util.Map<String, String> fieldErrors,
                                               int index) throws TransformException, IOException {
        IonStruct record = asStruct(value);
        DefaultRecordTransformer.TransformOutcome outcome = transformer.transformWithErrors(record);
        if (outcome.failed) {
            for (Entry<String, String> entry : outcome.fieldErrors.entrySet()) {
                fieldErrors.put(index + "." + entry.getKey(), entry.getValue());
            }
        }
        if (!outcome.dropped) {
            IonStruct output = outcome.record;
            if (output != null) {
                output.writeTo(writer);
                outputStream.write('\n');
            }
        }
        return new StreamCounters(index + 1, outcome.failed, outcome.dropped);
    }

    private record StreamCounters(int nextIndex, boolean failed, boolean dropped) {
    }

    private record StreamResult(TransformStats stats, URI uri) {
    }

    private List<IonStruct> normalizeRecords(Object rendered) throws TransformException {
        if (rendered == null) {
            return List.of();
        }
        if (rendered instanceof IonStruct ionStruct) {
            return List.of(ionStruct);
        }
        if (rendered instanceof IonList ionList) {
            List<IonStruct> records = new ArrayList<>();
            for (IonValue value : ionList) {
                IonStruct struct = asStruct(value);
                records.add(struct);
            }
            return records;
        }
        if (rendered instanceof List<?> list) {
            List<IonStruct> records = new ArrayList<>();
            for (Object value : list) {
                IonStruct struct = asStruct(IonValueUtils.toIonValue(value));
                records.add(struct);
            }
            return records;
        }
        if (rendered instanceof java.util.Map<?, ?> map) {
            IonStruct struct = asStruct(IonValueUtils.toIonValue(map));
            return List.of(struct);
        }
        throw new TransformException("Unsupported input type: " + rendered.getClass().getName()
            + ". The 'from' property must resolve to a list/map of records, Ion values, or a storage URI.");
    }

    private URI storeRecords(RunContext runContext, List<IonStruct> records) throws TransformException {
        String name = "transform-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream fileStream = java.nio.file.Files.newOutputStream(outputPath);
                 OutputStream outputStream = new java.io.BufferedOutputStream(fileStream, FileSerde.BUFFER_SIZE);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                for (IonStruct record : records) {
                    if (record == null) {
                        writer.writeNull();
                    } else {
                        record.writeTo(writer);
                    }
                    outputStream.write('\n');
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store transformed records", e);
        }
    }

    private ResolvedInput resolveInput(RunContext runContext) throws Exception {
        if (from == null) {
            return new ResolvedInput(null, false, null);
        }

        String expression = from.toString();
        if (expression != null && expression.contains("{{") && expression.contains("}}")) {
            Object typed = runContext.renderTyped(expression);
            if (typed != null && !(typed instanceof String)) {
                ResolvedInput resolved = resolveStorageCandidate(runContext, typed);
                if (resolved != null) {
                    return resolved;
                }
                return new ResolvedInput(typed, false, null);
            }
        }

        RunContextProperty<Object> rendered = runContext.render(from);
        Object value = rendered.as(Object.class).orElse(null);
        ResolvedInput resolved = resolveStorageCandidate(runContext, value);
        if (resolved != null) {
            return resolved;
        }
        if (!(value instanceof String)) {
            return new ResolvedInput(value, false, null);
        }
        try {
            Object listValue = rendered.asList(Object.class);
            if (listValue instanceof List) {
                return new ResolvedInput(listValue, false, null);
            }
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException ignored) {
        }
        try {
            Object mapValue = rendered.asMap(String.class, Object.class);
            if (mapValue instanceof java.util.Map) {
                return new ResolvedInput(mapValue, false, null);
            }
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException ignored) {
        }
        return new ResolvedInput(value, false, null);
    }

    private ResolvedInput resolveStorageCandidate(RunContext runContext, Object value) throws TransformException {
        if (value instanceof URI uriValue) {
            if (uriValue.getScheme() == null) {
                return new ResolvedInput(value, false, null);
            }
            return new ResolvedInput(uriValue, true, uriValue);
        }
        if (value instanceof String stringValue) {
            URI uri;
            try {
                uri = URI.create(stringValue);
            } catch (IllegalArgumentException e) {
                return null;
            }
            if (uri.getScheme() == null) {
                return null;
            }
            return new ResolvedInput(uri, true, uri);
        }
        return null;
    }

    private Object loadIonFromStorage(RunContext runContext, URI uri) throws TransformException {
        try (InputStream inputStream = runContext.storage().getFile(uri)) {
            IonList list = IonValueUtils.system().newEmptyList();
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            while (iterator.hasNext()) {
                list.add(IonValueUtils.cloneValue(iterator.next()));
            }
            return unwrapIonList(list);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }
    }

    private Object unwrapIonList(IonList list) {
        if (list == null || list.isEmpty()) {
            return List.of();
        }
        if (list.size() == 1) {
            IonValue value = list.get(0);
            if (value instanceof IonStruct || value instanceof IonList) {
                return value;
            }
        }
        return list;
    }

    private IonStruct asStruct(IonValue value) throws TransformException {
        if (value instanceof IonStruct struct) {
            return struct;
        }
        throw new TransformException("Expected struct record, got " + (value == null ? "null" : value.getType()));
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldDefinition {
        @Schema(title = "Expression")
        private String expr;

        @Schema(title = "Ion type")
        private IonTypeName type;

        @Builder.Default
        @Schema(title = "Optional")
        private boolean optional = false;

        @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
        public static FieldDefinition from(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof String stringValue) {
                return FieldDefinition.builder().expr(stringValue).build();
            }
            if (value instanceof java.util.Map<?, ?> map) {
                Object exprValue = map.get("expr");
                Object typeValue = map.get("type");
                Object optionalValue = map.get("optional");
                IonTypeName type = null;
                if (typeValue instanceof IonTypeName ionTypeName) {
                    type = ionTypeName;
                } else if (typeValue instanceof String typeString) {
                    type = IonTypeName.valueOf(typeString);
                }
                boolean optional = optionalValue instanceof Boolean bool ? bool : false;
                return FieldDefinition.builder()
                    .expr(exprValue == null ? null : String.valueOf(exprValue))
                    .type(type)
                    .optional(optional)
                    .build();
            }
            throw new IllegalArgumentException("Unsupported field definition: " + value);
        }

    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Options {
        @Builder.Default
        @Schema(title = "Keep unknown fields")
        private boolean keepUnknownFields = false;

        @Builder.Default
        @Schema(title = "Drop null fields")
        private boolean dropNulls = true;

        @Builder.Default
        @Schema(title = "On error behavior")
        private TransformOptions.OnErrorMode onError = TransformOptions.OnErrorMode.FAIL;

        TransformOptions toOptions() {
            return new TransformOptions(keepUnknownFields, dropNulls, onError);
        }
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE
    }

    private record ResolvedInput(Object value, boolean fromStorage, URI storageUri) {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Stored Ion file URI",
            description = "URI to the stored Ion file when output mode is STORE or AUTO resolves to STORE."
        )
        private final String uri;

        @Schema(
            title = "Transformed records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
