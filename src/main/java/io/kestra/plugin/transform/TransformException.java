package io.kestra.plugin.transform;

public class TransformException extends Exception {
    public TransformException(String message) {
        super(message);
    }

    public TransformException(String message, Throwable cause) {
        super(message, cause);
    }
}
