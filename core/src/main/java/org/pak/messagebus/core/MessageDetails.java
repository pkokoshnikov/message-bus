package org.pak.messagebus.core;

import lombok.Value;

import java.time.Instant;

@Value
public class MessageDetails<T> {
    String key;
    Instant originatedTime;
    T message;
}
