package org.pak.messagebus.core;

import java.time.Instant;

public record StdMessage<T>(String key, Instant originatedTime, T payload) implements Message<T> {
}
