package org.pak.messagebus.core;

import java.time.Instant;

public interface Message<T> {
    String key();

    Instant originatedTime();

    T payload();
}
