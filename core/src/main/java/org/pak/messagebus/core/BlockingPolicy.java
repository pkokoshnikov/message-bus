package org.pak.messagebus.core;

import lombok.NonNull;

import java.time.Duration;

public interface BlockingPolicy {
    boolean isBlocked(Exception exception);
    @NonNull
    Duration apply(Exception exception);
}
