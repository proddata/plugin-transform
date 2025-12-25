package io.kestra.plugin.transform;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import io.kestra.plugin.transform.engine.TransformStats;
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.expression.ExpressionException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Explode records",
    description = "Expand array fields into multiple records without scripts."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Explode items into rows",
            code = {
                "from: \"{{ outputs.fetch.records }}\"",
                "path: items[]",
                "as: item",
                "options:",
                "  keepUnknownFields: true",
                "  onError: FAIL"
            }
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE)
    }
)
public class Unnest extends Task implements RunnableTask<Unnest.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform, or a storage URI pointing to an Ion file."
    )
    private Property<Object> from;

    @Schema(
        title = "Array path",
        description = "Path expression to the array to explode (e.g., items[])."
    )
    private Property<String> path;

    @Schema(
        title = "Output field name",
        description = "Field name that holds the exploded element."
    )
    private Property<String> as;

    @Schema(
        title = "Options",
        description = "Behavior for unknown fields and error handling."
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

        String pathExpr = runContext.render(path).as(String.class).orElse(null);
        if (pathExpr == null || pathExpr.isBlank()) {
            throw new TransformException("path is required");
        }
        String asField = runContext.render(as).as(String.class).orElse(null);
        if (asField == null || asField.isBlank()) {
            throw new TransformException("as is required");
        }

        DefaultExpressionEngine expressionEngine = new DefaultExpressionEngine();
        StatsAccumulator stats = new StatsAccumulator();

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        if (resolvedInput.fromStorage() && effectiveOutput == OutputMode.STORE) {
            URI storedUri = unnestStreamToStorage(
                runContext,
                resolvedInput.storageUri(),
                pathExpr,
                asField,
                expressionEngine,
                stats
            );
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<IonStruct> records = normalizeRecords(resolveInMemory(runContext, resolvedInput));
        if (effectiveOutput == OutputMode.STORE) {
            URI storedUri = storeRecords(
                runContext,
                records,
                pathExpr,
                asField,
                expressionEngine,
                stats
            );
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("failed", stats.failed))
                .metric(Counter.of("dropped", stats.dropped));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<Object> rendered = expandToRecords(records, pathExpr, asField, expressionEngine, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("failed", stats.failed))
            .metric(Counter.of("dropped", stats.dropped));
        return Output.builder()
            .records(rendered)
            .build();
    }

    private Object resolveInMemory(RunContext runContext, ResolvedInput resolvedInput) throws TransformException {
        if (!resolvedInput.fromStorage()) {
            return resolvedInput.value();
        }
        return loadIonFromStorage(runContext, resolvedInput.storageUri());
    }

    private List<Object> expandToRecords(List<IonStruct> records,
                                         String pathExpr,
                                         String asField,
                                         DefaultExpressionEngine expressionEngine,
                                         StatsAccumulator stats) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            IonStruct record = records.get(i);
            stats.processed++;
            try {
                IonValue evaluated = expressionEngine.evaluate(pathExpr, record);
                if (IonValueUtils.isNull(evaluated)) {
                    continue;
                }
                if (!(evaluated instanceof IonList list)) {
                    throw new TransformException("Expected list at path: " + pathExpr);
                }
                if (list.isEmpty()) {
                    continue;
                }
                for (IonValue element : list) {
                    IonStruct output = buildOutputRecord(record, asField, element, options.keepUnknownFields);
                    outputRecords.add(IonValueUtils.toJavaValue(output));
                }
            } catch (ExpressionException | TransformException e) {
                stats.fail(i, "path", e.getMessage());
                if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                    stats.dropped++;
                    continue;
                }
                if (options.onError == TransformOptions.OnErrorMode.NULL) {
                    IonStruct output = buildOutputRecord(record, asField, IonValueUtils.nullValue(), options.keepUnknownFields);
                    outputRecords.add(IonValueUtils.toJavaValue(output));
                }
            }
        }
        return outputRecords;
    }

    private URI storeRecords(RunContext runContext,
                             List<IonStruct> records,
                             String pathExpr,
                             String asField,
                             DefaultExpressionEngine expressionEngine,
                             StatsAccumulator stats) throws TransformException {
        String name = "unnest-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = java.nio.file.Files.newOutputStream(outputPath);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                for (int i = 0; i < records.size(); i++) {
                    IonStruct record = records.get(i);
                    stats.processed++;
                    try {
                        IonValue evaluated = expressionEngine.evaluate(pathExpr, record);
                        if (IonValueUtils.isNull(evaluated)) {
                            continue;
                        }
                        if (!(evaluated instanceof IonList list)) {
                            throw new TransformException("Expected list at path: " + pathExpr);
                        }
                        if (list.isEmpty()) {
                            continue;
                        }
                        for (IonValue element : list) {
                            IonStruct output = buildOutputRecord(record, asField, element, options.keepUnknownFields);
                            output.writeTo(writer);
                            writer.flush();
                            outputStream.write('\n');
                        }
                    } catch (ExpressionException | TransformException e) {
                        stats.fail(i, "path", e.getMessage());
                        if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                            throw new TransformException(e.getMessage(), e);
                        }
                        if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                            stats.dropped++;
                            continue;
                        }
                        if (options.onError == TransformOptions.OnErrorMode.NULL) {
                            IonStruct output = buildOutputRecord(record, asField, IonValueUtils.nullValue(), options.keepUnknownFields);
                            output.writeTo(writer);
                            writer.flush();
                            outputStream.write('\n');
                        }
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store unnested records", e);
        }
    }

    private URI unnestStreamToStorage(RunContext runContext,
                                      URI uri,
                                      String pathExpr,
                                      String asField,
                                      DefaultExpressionEngine expressionEngine,
                                      StatsAccumulator stats) throws TransformException {
        String name = "unnest-" + UUID.randomUUID() + ".ion";
        InputStream inputStream;
        try {
            inputStream = runContext.storage().getFile(uri);
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }

        try (InputStream stream = inputStream) {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = java.nio.file.Files.newOutputStream(outputPath);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                Iterator<IonValue> iterator = IonValueUtils.system().iterate(stream);
                int index = 0;
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            index = processStreamRecord(element, index, pathExpr, asField, expressionEngine, stats, writer, outputStream);
                        }
                    } else {
                        index = processStreamRecord(value, index, pathExpr, asField, expressionEngine, stats, writer, outputStream);
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store unnested records", e);
        }
    }

    private int processStreamRecord(IonValue value,
                                    int index,
                                    String pathExpr,
                                    String asField,
                                    DefaultExpressionEngine expressionEngine,
                                    StatsAccumulator stats,
                                    IonWriter writer,
                                    OutputStream outputStream) throws TransformException, IOException {
        IonStruct record = asStruct(value);
        stats.processed++;
        try {
            IonValue evaluated = expressionEngine.evaluate(pathExpr, record);
            if (IonValueUtils.isNull(evaluated)) {
                return index + 1;
            }
            if (!(evaluated instanceof IonList list)) {
                throw new TransformException("Expected list at path: " + pathExpr);
            }
            if (list.isEmpty()) {
                return index + 1;
            }
            for (IonValue element : list) {
                IonStruct output = buildOutputRecord(record, asField, element, options.keepUnknownFields);
                output.writeTo(writer);
                writer.flush();
                outputStream.write('\n');
            }
        } catch (ExpressionException | TransformException e) {
            stats.fail(index, "path", e.getMessage());
            if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                throw new TransformException(e.getMessage(), e);
            }
            if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                stats.dropped++;
                return index + 1;
            }
            if (options.onError == TransformOptions.OnErrorMode.NULL) {
                IonStruct output = buildOutputRecord(record, asField, IonValueUtils.nullValue(), options.keepUnknownFields);
                output.writeTo(writer);
                writer.flush();
                outputStream.write('\n');
            }
        }
        return index + 1;
    }

    private IonStruct buildOutputRecord(IonStruct input, String asField, IonValue element, boolean keepUnknownFields) {
        IonStruct output = IonValueUtils.system().newEmptyStruct();
        if (keepUnknownFields) {
            for (IonValue value : input) {
                output.put(value.getFieldName(), IonValueUtils.cloneValue(value));
            }
        }
        output.put(asField, IonValueUtils.cloneValue(element == null ? IonValueUtils.nullValue() : element));
        return output;
    }

    private static final class StatsAccumulator {
        private int processed;
        private int failed;
        private int dropped;
        private final Map<String, String> fieldErrors = new HashMap<>();

        private void fail(int index, String field, String message) {
            failed++;
            fieldErrors.put(index + "." + field, message);
        }

        private TransformStats snapshot() {
            return new TransformStats(processed, failed, dropped, new HashMap<>(fieldErrors));
        }
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
        if (rendered instanceof Map<?, ?> map) {
            IonStruct struct = asStruct(IonValueUtils.toIonValue(map));
            return List.of(struct);
        }
        throw new TransformException("Unsupported input type: " + rendered.getClass().getName()
            + ". The 'from' property must resolve to a list/map of records, Ion values, or a storage URI.");
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
            if (mapValue instanceof Map) {
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
    public static class Options {
        @Builder.Default
        @Schema(title = "Keep unknown fields")
        private boolean keepUnknownFields = true;

        @Builder.Default
        @Schema(title = "On error behavior")
        private TransformOptions.OnErrorMode onError = TransformOptions.OnErrorMode.FAIL;
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
            title = "Unnested records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
