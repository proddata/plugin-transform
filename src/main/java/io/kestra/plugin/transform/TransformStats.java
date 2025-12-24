package io.kestra.plugin.transform;

public record TransformStats(
    int processed,
    int failed,
    int dropped,
    java.util.Map<String, String> fieldErrors
) {
}
