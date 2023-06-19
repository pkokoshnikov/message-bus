package org.pak.messagebus.core.error;

public class PersistenceException extends RuntimeException {
    public PersistenceException(Throwable cause) {
        super(cause);
    }
}
