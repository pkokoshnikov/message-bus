package org.pak.messagebus.core;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class SubscriptionName {
    private final String subscriptionName;

    public SubscriptionName(String subscriptionName) {
        if (!subscriptionName.matches("^[a-z-]+$")) {
            throw new IllegalArgumentException("Subscription name must be lowercase and -");
        }

        this.subscriptionName = subscriptionName;
    }

    public String name() {
        return subscriptionName;
    }
}
