package org.pak.messagebus.core.error;

public class NonRetrayablePersistenceException extends PersistenceException {
    public NonRetrayablePersistenceException(Throwable cause) {
        super(cause);
    }
}
