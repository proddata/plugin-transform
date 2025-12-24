package io.kestra.plugin.transform;

public record FieldMapping(
    String targetField,
    String expression,
    IonTypeName type,
    boolean optional
) {
}
