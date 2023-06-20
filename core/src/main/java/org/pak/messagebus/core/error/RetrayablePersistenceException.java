package org.pak.messagebus.core.error;

public class RetrayablePersistenceException extends PersistenceException {
    public RetrayablePersistenceException(Throwable cause) {
        super(cause);
    }
}
