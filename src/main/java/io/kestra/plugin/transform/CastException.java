package io.kestra.plugin.transform;

public class CastException extends Exception {
    public CastException(String message) {
        super(message);
    }

    public CastException(String message, Throwable cause) {
        super(message, cause);
    }
}
