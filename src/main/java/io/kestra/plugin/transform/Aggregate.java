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
import io.kestra.plugin.transform.expression.DefaultExpressionEngine;
import io.kestra.plugin.transform.expression.ExpressionException;
import io.kestra.plugin.transform.ion.CastException;
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
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Aggregate records",
    description = "Group records and compute typed aggregates."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Aggregate totals",
            code = {
                "from: \"{{ outputs.normalize.records }}\"",
                "groupBy:",
                "  - customer_id",
                "  - country",
                "aggregates:",
                "  order_count:",
                "    expr: count()",
                "    type: INT",
                "  total_spent:",
                "    expr: sum(total_spent)",
                "    type: DECIMAL",
                "options:",
                "  onError: FAIL"
            }
        ),
        @io.kestra.core.models.annotations.Example(
            title = "Aggregate with stored output",
            full = true,
            code = """
                id: aggregate_totals
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.core.output.OutputValues
                    values:
                      records:
                        - customer_id: "c1"
                          country: "FR"
                          total_spent: 10
                        - customer_id: "c1"
                          country: "FR"
                          total_spent: 5

                  - id: aggregate
                    type: io.kestra.plugin.transform.Aggregate
                    from: "{{ outputs.fetch.values.records }}"
                    output: STORE
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
                """
        )
    },
    metrics = {
        @Metric(name = "processed", type = Counter.TYPE),
        @Metric(name = "groups", type = Counter.TYPE),
        @Metric(name = "failed", type = Counter.TYPE)
    }
)
public class Aggregate extends Task implements RunnableTask<Aggregate.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform, or a storage URI pointing to an Ion file."
    )
    private Property<Object> from;

    @Schema(
        title = "Group by fields",
        description = "Fields to group on."
    )
    private Property<List<String>> groupBy;

    @Schema(
        title = "Aggregate definitions",
        description = "Aggregate expressions with optional types."
    )
    private Map<String, AggregateDefinition> aggregates;

    @Schema(
        title = "Options",
        description = "Output and error handling options."
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

        List<String> groupByFields = runContext.render(groupBy).asList(String.class);
        if (groupByFields == null || groupByFields.isEmpty()) {
            throw new TransformException("groupBy is required");
        }
        if (aggregates == null || aggregates.isEmpty()) {
            throw new TransformException("aggregates is required");
        }

        List<AggregateMapping> mappings = new ArrayList<>();
        for (Map.Entry<String, AggregateDefinition> entry : aggregates.entrySet()) {
            AggregateDefinition definition = entry.getValue();
            if (definition == null) {
                throw new TransformException("Aggregate definition is required for '" + entry.getKey() + "'");
            }
            mappings.add(new AggregateMapping(
                entry.getKey(),
                Objects.requireNonNull(definition.expr, "expr is required"),
                definition.type
            ));
        }

        DefaultExpressionEngine expressionEngine = new DefaultExpressionEngine();
        DefaultIonCaster caster = new DefaultIonCaster();
        StatsAccumulator stats = new StatsAccumulator();

        Map<GroupKey, GroupBucket> grouped = new LinkedHashMap<>();
        if (resolvedInput.fromStorage()) {
            groupFromStorage(runContext, resolvedInput.storageUri(), groupByFields, grouped, stats);
        } else {
            List<IonStruct> records = normalizeRecords(resolvedInput.value());
            groupRecords(records, groupByFields, grouped, stats);
        }

        OutputMode effectiveOutput = output == OutputMode.AUTO
            ? (resolvedInput.fromStorage() ? OutputMode.STORE : OutputMode.RECORDS)
            : output;

        if (effectiveOutput == OutputMode.STORE) {
            URI stored = storeRecords(runContext, grouped, groupByFields, mappings, expressionEngine, caster, stats);
            runContext.metric(Counter.of("processed", stats.processed))
                .metric(Counter.of("groups", stats.groups))
                .metric(Counter.of("failed", stats.failed));
            return Output.builder()
                .uri(stored.toString())
                .build();
        }

        List<Object> rendered = aggregateToRecords(grouped, groupByFields, mappings, expressionEngine, caster, stats);
        runContext.metric(Counter.of("processed", stats.processed))
            .metric(Counter.of("groups", stats.groups))
            .metric(Counter.of("failed", stats.failed));
        return Output.builder()
            .records(rendered)
            .build();
    }

    private List<Object> aggregateToRecords(Map<GroupKey, GroupBucket> grouped,
                                            List<String> groupByFields,
                                            List<AggregateMapping> mappings,
                                            DefaultExpressionEngine expressionEngine,
                                            DefaultIonCaster caster,
                                            StatsAccumulator stats) throws TransformException {
        List<Object> outputRecords = new ArrayList<>();
        for (GroupBucket bucket : grouped.values()) {
            IonStruct output = aggregateBucket(bucket, groupByFields, mappings, expressionEngine, caster, stats);
            if (output != null) {
                outputRecords.add(IonValueUtils.toJavaValue(output));
            }
        }
        return outputRecords;
    }

    private void groupRecords(List<IonStruct> records,
                              List<String> groupByFields,
                              Map<GroupKey, GroupBucket> grouped,
                              StatsAccumulator stats) throws TransformException {
        for (IonStruct record : records) {
            stats.processed++;
            GroupKey key = buildGroupKey(record, groupByFields);
            grouped.computeIfAbsent(key, k -> new GroupBucket(record, groupByFields)).records.add(record);
        }
    }

    private void groupFromStorage(RunContext runContext,
                                  URI uri,
                                  List<String> groupByFields,
                                  Map<GroupKey, GroupBucket> grouped,
                                  StatsAccumulator stats) throws TransformException {
        try (InputStream inputStream = runContext.storage().getFile(uri)) {
            Iterator<IonValue> iterator = IonValueUtils.system().iterate(inputStream);
            while (iterator.hasNext()) {
                IonValue value = iterator.next();
                if (value instanceof IonList list) {
                    for (IonValue element : list) {
                        groupStreamRecord(element, groupByFields, grouped, stats);
                    }
                } else {
                    groupStreamRecord(value, groupByFields, grouped, stats);
                }
            }
        } catch (IOException e) {
            throw new TransformException("Unable to read Ion file from storage: " + uri, e);
        }
    }

    private void groupStreamRecord(IonValue value,
                                   List<String> groupByFields,
                                   Map<GroupKey, GroupBucket> grouped,
                                   StatsAccumulator stats) throws TransformException {
        IonStruct record = asStruct(value);
        stats.processed++;
        GroupKey key = buildGroupKey(record, groupByFields);
        grouped.computeIfAbsent(key, k -> new GroupBucket(record, groupByFields)).records.add(record);
    }

    private URI storeRecords(RunContext runContext,
                             Map<GroupKey, GroupBucket> grouped,
                             List<String> groupByFields,
                             List<AggregateMapping> mappings,
                             DefaultExpressionEngine expressionEngine,
                             DefaultIonCaster caster,
                             StatsAccumulator stats) throws TransformException {
        String name = "aggregate-" + UUID.randomUUID() + ".ion";
        try {
            java.nio.file.Path outputPath = runContext.workingDir().createTempFile(".ion");
            try (OutputStream outputStream = java.nio.file.Files.newOutputStream(outputPath);
                 IonWriter writer = IonValueUtils.system().newTextWriter(outputStream)) {
                for (GroupBucket bucket : grouped.values()) {
                    IonStruct output = aggregateBucket(bucket, groupByFields, mappings, expressionEngine, caster, stats);
                    if (output != null) {
                        output.writeTo(writer);
                        writer.flush();
                        outputStream.write('\n');
                    }
                }
                writer.finish();
            }
            return runContext.storage().putFile(outputPath.toFile(), name);
        } catch (IOException e) {
            throw new TransformException("Unable to store aggregated records", e);
        }
    }

    private IonStruct aggregateBucket(GroupBucket bucket,
                                      List<String> groupByFields,
                                      List<AggregateMapping> mappings,
                                      DefaultExpressionEngine expressionEngine,
                                      DefaultIonCaster caster,
                                      StatsAccumulator stats) throws TransformException {
        IonStruct output = IonValueUtils.system().newEmptyStruct();
        for (String field : groupByFields) {
            IonValue value = bucket.groupValues.get(field);
            output.put(field, IonValueUtils.cloneValue(value));
        }

        boolean failed = false;
        for (AggregateMapping mapping : mappings) {
            try {
                IonValue aggregated = evaluateAggregate(mapping.expr, bucket.records, expressionEngine);
                IonValue casted = aggregated;
                if (mapping.type != null && !IonValueUtils.isNull(aggregated)) {
                    casted = caster.cast(aggregated, mapping.type);
                }
                output.put(mapping.targetField, IonValueUtils.cloneValue(casted));
            } catch (ExpressionException | TransformException | CastException e) {
                failed = true;
                stats.failed++;
                if (options.onError == TransformOptions.OnErrorMode.FAIL) {
                    throw new TransformException(e.getMessage(), e);
                }
                if (options.onError == TransformOptions.OnErrorMode.SKIP) {
                    return null;
                }
                if (options.onError == TransformOptions.OnErrorMode.NULL) {
                    output.put(mapping.targetField, IonValueUtils.nullValue());
                }
            }
        }

        if (!failed || options.onError != TransformOptions.OnErrorMode.SKIP) {
            stats.groups++;
        }
        return output;
    }

    private IonValue evaluateAggregate(String expression,
                                       List<IonStruct> records,
                                       DefaultExpressionEngine expressionEngine) throws ExpressionException, TransformException {
        String trimmed = expression == null ? "" : expression.trim();
        if ("count()".equals(trimmed)) {
            return IonValueUtils.system().newInt(records.size());
        }
        if (trimmed.startsWith("sum(")) {
            String arg = extractArgument(expression);
            if (arg.isEmpty()) {
                throw new TransformException("sum requires an argument");
            }
            return IonValueUtils.system().newDecimal(sum(valuesFor(arg, records, expressionEngine)));
        }
        if (trimmed.startsWith("min(")) {
            String arg = extractArgument(expression);
            if (arg.isEmpty()) {
                throw new TransformException("min requires an argument");
            }
            BigDecimal min = min(valuesFor(arg, records, expressionEngine));
            return min == null ? IonValueUtils.nullValue() : IonValueUtils.system().newDecimal(min);
        }
        if (trimmed.startsWith("max(")) {
            String arg = extractArgument(expression);
            if (arg.isEmpty()) {
                throw new TransformException("max requires an argument");
            }
            BigDecimal max = max(valuesFor(arg, records, expressionEngine));
            return max == null ? IonValueUtils.nullValue() : IonValueUtils.system().newDecimal(max);
        }
        if (trimmed.startsWith("avg(")) {
            String arg = extractArgument(expression);
            if (arg.isEmpty()) {
                throw new TransformException("avg requires an argument");
            }
            List<BigDecimal> values = valuesFor(arg, records, expressionEngine);
            if (values.isEmpty()) {
                return IonValueUtils.nullValue();
            }
            BigDecimal sum = sum(values);
            return IonValueUtils.system().newDecimal(sum.divide(BigDecimal.valueOf(values.size()), java.math.RoundingMode.HALF_UP));
        }
        if (trimmed.startsWith("first(")) {
            String arg = extractArgument(expression);
            if (arg.isEmpty()) {
                throw new TransformException("first requires an argument");
            }
            IonValue value = firstValue(arg, records, expressionEngine);
            return value == null ? IonValueUtils.nullValue() : IonValueUtils.cloneValue(value);
        }
        if (trimmed.startsWith("last(")) {
            String arg = extractArgument(expression);
            if (arg.isEmpty()) {
                throw new TransformException("last requires an argument");
            }
            IonValue value = lastValue(arg, records, expressionEngine);
            return value == null ? IonValueUtils.nullValue() : IonValueUtils.cloneValue(value);
        }
        throw new TransformException("Unsupported aggregate expression: " + expression);
    }

    private List<BigDecimal> valuesFor(String arg,
                                       List<IonStruct> records,
                                       DefaultExpressionEngine expressionEngine) throws ExpressionException, TransformException {
        List<BigDecimal> values = new ArrayList<>();
        for (IonStruct record : records) {
            IonValue value = expressionEngine.evaluate(arg, record);
            if (IonValueUtils.isNull(value)) {
                continue;
            }
            try {
                BigDecimal decimal = IonValueUtils.asDecimal(value);
                if (decimal != null) {
                    values.add(decimal);
                }
            } catch (CastException e) {
                throw new TransformException("Expected numeric value for aggregate " + arg, e);
            }
        }
        return values;
    }

    private IonValue firstValue(String arg,
                                List<IonStruct> records,
                                DefaultExpressionEngine expressionEngine) throws ExpressionException {
        for (IonStruct record : records) {
            IonValue value = expressionEngine.evaluate(arg, record);
            if (!IonValueUtils.isNull(value)) {
                return value;
            }
        }
        return null;
    }

    private IonValue lastValue(String arg,
                               List<IonStruct> records,
                               DefaultExpressionEngine expressionEngine) throws ExpressionException {
        IonValue last = null;
        for (IonStruct record : records) {
            IonValue value = expressionEngine.evaluate(arg, record);
            if (!IonValueUtils.isNull(value)) {
                last = value;
            }
        }
        return last;
    }

    private String extractArgument(String expression) throws TransformException {
        int start = expression.indexOf('(');
        int end = expression.lastIndexOf(')');
        if (start < 0 || end < 0 || end < start) {
            return "";
        }
        return expression.substring(start + 1, end).trim();
    }

    private BigDecimal sum(List<BigDecimal> values) {
        BigDecimal total = BigDecimal.ZERO;
        for (BigDecimal value : values) {
            total = total.add(value);
        }
        return total;
    }

    private BigDecimal min(List<BigDecimal> values) {
        BigDecimal min = null;
        for (BigDecimal value : values) {
            min = min == null ? value : min.min(value);
        }
        return min;
    }

    private BigDecimal max(List<BigDecimal> values) {
        BigDecimal max = null;
        for (BigDecimal value : values) {
            max = max == null ? value : max.max(value);
        }
        return max;
    }

    private GroupKey buildGroupKey(IonStruct record, List<String> groupByFields) throws TransformException {
        List<Object> key = new ArrayList<>();
        for (String field : groupByFields) {
            IonValue value = record.get(field);
            key.add(IonValueUtils.toJavaValue(value));
        }
        return new GroupKey(key);
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
    public static class AggregateDefinition {
        @Schema(title = "Expression")
        private String expr;

        @Schema(title = "Ion type")
        private IonTypeName type;

        @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
        public static AggregateDefinition from(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof String stringValue) {
                return AggregateDefinition.builder().expr(stringValue).build();
            }
            if (value instanceof Map<?, ?> map) {
                Object exprValue = map.get("expr");
                Object typeValue = map.get("type");
                IonTypeName type = null;
                if (typeValue instanceof IonTypeName ionTypeName) {
                    type = ionTypeName;
                } else if (typeValue instanceof String typeString) {
                    type = IonTypeName.valueOf(typeString);
                }
                return AggregateDefinition.builder()
                    .expr(exprValue == null ? null : String.valueOf(exprValue))
                    .type(type)
                    .build();
            }
            throw new IllegalArgumentException("Unsupported aggregate definition: " + value);
        }
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Options {
        @Builder.Default
        @Schema(title = "On error behavior")
        private TransformOptions.OnErrorMode onError = TransformOptions.OnErrorMode.FAIL;
    }

    public enum OutputMode {
        AUTO,
        RECORDS,
        STORE
    }

    private record AggregateMapping(String targetField, String expr, IonTypeName type) {
    }

    private record GroupKey(List<Object> values) {
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey that)) {
                return false;
            }
            return Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(values);
        }
    }

    private static final class GroupBucket {
        private final List<IonStruct> records = new ArrayList<>();
        private final Map<String, IonValue> groupValues = new LinkedHashMap<>();

        private GroupBucket(IonStruct record, List<String> groupByFields) {
            for (String field : groupByFields) {
                groupValues.put(field, record.get(field));
            }
        }
    }

    private record ResolvedInput(Object value, boolean fromStorage, URI storageUri) {
    }

    private static final class StatsAccumulator {
        private int processed;
        private int groups;
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
            title = "Aggregated records",
            description = "JSON-safe records when output mode is RECORDS or AUTO resolves to RECORDS."
        )
        private final List<Object> records;
    }
}
