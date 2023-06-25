package org.pak.messagebus.core;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class MessageName {
    private final String messageName;

    public MessageName(String messageName) {
        if (!messageName.matches("^[a-z-]+$")){
            throw new IllegalArgumentException("Event name must be lowercase and -");
        }

        this.messageName = messageName;
    }

    public String name() {
        return messageName;
    }
}
