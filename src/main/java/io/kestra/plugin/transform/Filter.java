package io.kestra.plugin.transform;

import com.amazon.ion.IonBool;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Filter records",
    description = "Keep or drop records based on a boolean expression."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Keep active customers",
            code = {
                "from: \"{{ outputs.normalize.records }}\"",
                "where: is_active && total_spent > 100",
                "options:",
                "  onError: SKIP"
            }
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "passed", type = Counter.TYPE),
        @Metric(name = "dropped", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE)
    }
)
public class Filter extends Task implements RunnableTask<Filter.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform, or a storage URI pointing to an Ion file."
    )
    private Property<Object> from;

    @Schema(
        title = "Filter expression",
        description = "Boolean expression evaluated on each record."
    )
    private Property<String> where;

    @Schema(
        title = "Options",
        description = "Error handling behavior."
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

        String whereExpr = runContext.render(where).as(String.class).orElse(null);
        if (whereExpr == null || whereExpr.isBlank()) {
            throw new TransformException("where is required");
        }

        DefaultExpressionEngine expressionEngine = new DefaultExpressionEngine();
        StatsAccumulator stats = new StatsAccumulator();

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        if (resolvedInput.fromStorage() && effectiveOutput == OutputMode.STORE) {
            URI storedUri = filterStreamToStorage(runContext, resolvedInput.storageUri(), whereExpr, expressionEngine, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("passed", stats.passed))
                .metric(Counter.of("dropped", stats.dropped))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<IonStruct> records = normalizeRecords(resolveInMemory(runContext, resolvedInput));
        if (effectiveOutput == OutputMode.STORE) {
            URI storedUri = storeRecords(runContext, records, whereExpr, expressionEngine, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("passed", stats.passed))
                .metric(Counter.of("dropped", stats.dropped))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(storedUri.toString())
                .build();
        }

        List<Object> rendered = filterToRecords(records, whereExpr, expressionEngine, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("passed", stats.passed))
            .metric(Counter.of("dropped", stats.dropped))
            .metric(Counter.of("failed", stats.failed));
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

    private List<Object> filterToRecords(List<IonStruct> records,
                                         String whereExpr,
                                         DefaultExpressionEngine expressionEngine,
                                         StatsAccumulator stats) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            IonStruct record = records.get(i);
            stats.processed++;
            try {
                Boolean decision = evaluateBoolean(whereExpr, record, expressionEngine);
                if (decision) {
                    stats.passed++;
                    outputRecords.add(IonValueUtils.toJavaValue(record));
                } else {
                    stats.dropped++;
                }
            } catch (ExpressionException | TransformException e) {
                stats.failed++;
                if (options.onError == OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (options.onError == OnErrorMode.SKIP) {
                    stats.dropped++;
                    continue;
                }
                if (options.onError == OnErrorMode.KEEP) {
                    stats.passed++;
                    outputRecords.add(IonValueUtils.toJavaValue(record));
                }
            }
        }
        return outputRecords;
    }

    private URI storeRecords(RunContext runContext,
                             List<IonStruct> records,
                             String whereExpr,
                             DefaultExpressionEngine expressionEngine,
                             StatsAccumulator stats) throws TransformException {
        String name = "filter-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = java.nio.file.Files.newOutputStream(outputPath);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                for (int i = 0; i < records.size(); i++) {
                    IonStruct record = records.get(i);
                    stats.processed++;
                    try {
                        Boolean decision = evaluateBoolean(whereExpr, record, expressionEngine);
                        if (decision) {
                            stats.passed++;
                            record.writeTo(writer);
                            outputStream.write('\n');
                        } else {
                            stats.dropped++;
                        }
                    } catch (ExpressionException | TransformException e) {
                        stats.failed++;
                        if (options.onError == OnErrorMode.FAIL) {
                            throw new TransformException(e.getMessage(), e);
                        }
                        if (options.onError == OnErrorMode.SKIP) {
                            stats.dropped++;
                            continue;
                        }
                        if (options.onError == OnErrorMode.KEEP) {
                            stats.passed++;
                            record.writeTo(writer);
                            outputStream.write('\n');
                        }
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store filtered records", e);
        }
    }

    private URI filterStreamToStorage(RunContext runContext,
                                      URI uri,
                                      String whereExpr,
                                      DefaultExpressionEngine expressionEngine,
                                      StatsAccumulator stats) throws TransformException {
        String name = "filter-" + UUID.randomUUID() + ".ion";
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
                while (iterator.hasNext()) {
                    IonValue value = iterator.next();
                    if (value instanceof IonList list) {
                        for (IonValue element : list) {
                            filterStreamRecord(element, whereExpr, expressionEngine, stats, writer, outputStream);
                        }
                    } else {
                        filterStreamRecord(value, whereExpr, expressionEngine, stats, writer, outputStream);
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store filtered records", e);
        }
    }

    private void filterStreamRecord(IonValue value,
                                    String whereExpr,
                                    DefaultExpressionEngine expressionEngine,
                                    StatsAccumulator stats,
                                    IonWriter writer,
                                    OutputStream outputStream) throws TransformException, IOException {
        IonStruct record = asStruct(value);
        stats.processed++;
        try {
            Boolean decision = evaluateBoolean(whereExpr, record, expressionEngine);
            if (decision) {
                stats.passed++;
                record.writeTo(writer);
                outputStream.write('\n');
            } else {
                stats.dropped++;
            }
        } catch (ExpressionException | TransformException e) {
            stats.failed++;
            if (options.onError == OnErrorMode.FAIL) {
                throw new TransformException(e.getMessage(), e);
            }
            if (options.onError == OnErrorMode.SKIP) {
                stats.dropped++;
                return;
            }
            if (options.onError == OnErrorMode.KEEP) {
                stats.passed++;
                record.writeTo(writer);
                outputStream.write('\n');
            }
        }
    }

    private Boolean evaluateBoolean(String whereExpr,
                                    IonStruct record,
                                    DefaultExpressionEngine expressionEngine) throws ExpressionException, TransformException {
        IonValue evaluated = expressionEngine.evaluate(whereExpr, record);
        if (IonValueUtils.isNull(evaluated)) {
            throw new TransformException("where expression evaluated to null");
        }
        if (evaluated instanceof IonBool ionBool) {
            return ionBool.booleanValue();
        }
        try {
            Boolean value = IonValueUtils.asBoolean(evaluated);
            if (value == null) {
                throw new TransformException("where expression evaluated to null");
            }
            return value;
        } catch (io.kestra.plugin.transform.ion.CastException e) {
            throw new TransformException("where expression must return boolean, got " + evaluated.getType(), e);
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
        @Schema(title = "On error behavior")
        private OnErrorMode onError = OnErrorMode.FAIL;
    }

    public enum OnErrorMode {
        FAIL,
        SKIP,
        KEEP
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE
    }

    private record ResolvedInput(Object value, boolean fromStorage, URI storageUri) {
    }

    private static final class StatsAccumulator {
        private int processed;
        private int passed;
        private int dropped;
        private int failed;
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
            title = "Filtered records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
