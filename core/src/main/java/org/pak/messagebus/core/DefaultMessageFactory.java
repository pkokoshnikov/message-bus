package org.pak.messagebus.core;

import java.time.Instant;

public class DefaultMessageFactory implements MessageFactory {
    @Override
    public <T> Message<T> createMessage(String key, Instant originatedTime, T payload) {
        return new DefaultMessage<>(key, originatedTime, payload);
    }
}
