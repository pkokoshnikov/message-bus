package org.pak.messagebus.core.error;

public class RetrayablePersistenceException extends PersistenceException {
    public RetrayablePersistenceException(Throwable cause, Throwable originalCause) {
        super(cause, originalCause);
    }
}
