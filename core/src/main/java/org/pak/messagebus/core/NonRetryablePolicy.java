package org.pak.messagebus.core;

public interface NonRetryablePolicy {
    boolean isNonRetryable(Exception exception);
}
