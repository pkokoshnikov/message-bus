package org.pak.messagebus.core;

import java.time.Duration;

public interface RetryablePolicy {
    Duration apply(Exception e, Integer attempt);
}
