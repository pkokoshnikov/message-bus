package org.pak.messagebus.core;

import lombok.EqualsAndHashCode;
import lombok.experimental.FieldDefaults;

@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@EqualsAndHashCode
public class SubscriptionType<T extends Message> {
    MessageType<T> messageType;
    String subscriptionName;

    public SubscriptionType(String subscriptionName, MessageType<T> messageType) {
        if (!subscriptionName.matches("^[a-z-]+$")) {
            throw new IllegalArgumentException("Subscription name must be lowercase and -");
        }

        this.messageType = messageType;
        this.subscriptionName = subscriptionName;
    }

    public String name() {
        return subscriptionName;
    }
}
