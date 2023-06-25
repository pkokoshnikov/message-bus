package org.pak.messagebus.core;

public class StdNonRetryablePolicy implements NonRetryablePolicy {

    @Override
    public boolean isNonRetryable(Exception exception) {
        return false;
    }
}
