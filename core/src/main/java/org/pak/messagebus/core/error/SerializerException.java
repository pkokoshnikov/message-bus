package org.pak.messagebus.core.error;

public class SerializerException extends RuntimeException {
    public SerializerException(Throwable throwable) {
        super(throwable);
    }
}
