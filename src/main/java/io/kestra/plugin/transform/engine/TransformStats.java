package io.kestra.plugin.transform.engine;

public record TransformStats(
    int processed,
    int failed,
    int dropped,
    java.util.Map<String, String> fieldErrors
) {
}
