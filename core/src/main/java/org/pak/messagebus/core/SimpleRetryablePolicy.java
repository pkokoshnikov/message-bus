package org.pak.messagebus.core;

import java.time.Duration;

public class SimpleRetryablePolicy implements RetryablePolicy {
    @Override
    public Duration apply(Exception e, Integer attempt) {
        return Duration.ofSeconds(Math.min((long) Math.pow(2, attempt), 10 * 60));
    }
}
