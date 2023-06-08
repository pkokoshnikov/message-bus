package org.pak.messagebus.core.error;

public class CoreException extends RuntimeException{
    public CoreException(Throwable throwable) {
        super(throwable);
    }
    public CoreException(String message) {
        super(message);
    }

    public CoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
