package org.pak.messagebus.core;

import java.time.Duration;

public interface BlockingPolicy {
    Duration apply(Exception exception);
}
