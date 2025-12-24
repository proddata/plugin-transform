package io.kestra.plugin.transform;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

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
        )
    }
)
public class Map extends Task implements RunnableTask<Map.Output> {
    @Schema(
        title = "Input records",
        description = "Ion list or struct to transform."
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        Object rendered = resolveInput(runContext);
        List<IonStruct> records = normalizeRecords(rendered);

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
        DefaultTransformTaskEngine engine = new DefaultTransformTaskEngine(transformer);
        TransformResult result = engine.execute(records);

        List<Object> renderedRecords = new ArrayList<>();
        for (IonStruct record : result.records()) {
            renderedRecords.add(IonValueUtils.toJavaValue(record));
        }

        return Output.builder()
            .records(renderedRecords)
            .stats(result.stats())
            .build();
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
        throw new TransformException("Unsupported input type: " + rendered.getClass().getName());
    }

    private Object resolveInput(RunContext runContext) throws io.kestra.core.exceptions.IllegalVariableEvaluationException {
        if (from == null) {
            return null;
        }

        String expression = from.toString();
        if (expression != null && expression.contains("{{") && expression.contains("}}")) {
            Object typed = runContext.renderTyped(expression);
            if (typed != null && !(typed instanceof String)) {
                return typed;
            }
        }

        RunContextProperty<Object> rendered = runContext.render(from);
        Object value = rendered.as(Object.class).orElse(null);
        if (!(value instanceof String)) {
            return value;
        }
        try {
            Object listValue = rendered.asList(Object.class);
            if (listValue instanceof List) {
                return listValue;
            }
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException ignored) {
        }
        try {
            Object mapValue = rendered.asMap(String.class, Object.class);
            if (mapValue instanceof java.util.Map) {
                return mapValue;
            }
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException ignored) {
        }
        return value;
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

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final List<Object> records;
        private final TransformStats stats;
    }
}
