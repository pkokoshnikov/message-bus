package org.pak.messagebus.core;

import java.time.Duration;

import static org.pak.messagebus.core.MessageProcessor.DEFAULT_BLOCKING_DURATION;

public class SimpleBlockingPolicy implements BlockingPolicy {

    @Override
    public Duration apply(Exception exception) {
        return DEFAULT_BLOCKING_DURATION;
    }
}
