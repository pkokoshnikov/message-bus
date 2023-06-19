package org.pak.messagebus.core.error;

public class IncorrectStateException extends RuntimeException {
    public IncorrectStateException(Throwable throwable) {
        super(throwable);
    }

    public IncorrectStateException(String message) {
        super(message);
    }

    public IncorrectStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
