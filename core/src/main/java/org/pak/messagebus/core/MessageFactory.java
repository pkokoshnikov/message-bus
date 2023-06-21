package org.pak.messagebus.core;

import java.time.Instant;

public interface MessageFactory {
    <T> Message<T> createMessage(String key, Instant originatedTime, T payload);
}
