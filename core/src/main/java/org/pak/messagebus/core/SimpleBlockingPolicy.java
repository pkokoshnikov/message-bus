package org.pak.messagebus.core;

import java.time.Duration;

public class SimpleBlockingPolicy implements BlockingPolicy {

    @Override
    public Duration apply(Exception exception) {
        return Duration.ofSeconds(60);
    }
}
