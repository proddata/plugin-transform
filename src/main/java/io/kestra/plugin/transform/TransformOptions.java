package io.kestra.plugin.transform;

public record TransformOptions(
    boolean keepUnknownFields,
    boolean dropNulls,
    OnErrorMode onError
) {
    public enum OnErrorMode {
        FAIL,
        SKIP,
        NULL
    }
}
