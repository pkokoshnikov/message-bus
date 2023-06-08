package org.pak.messagebus.core;

import lombok.EqualsAndHashCode;
import lombok.experimental.FieldDefaults;

@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@EqualsAndHashCode
public class MessageType<T extends Message> {
    String messageName;
    Class<T> clazz;

    public MessageType(String messageName, Class<T> clazz) {
        this.clazz = clazz;
        if (!messageName.matches("^[a-z-]+$")){
            throw new IllegalArgumentException("Event name must be lowercase and -");
        }

        this.messageName = messageName;
    }

    public String name() {
        return messageName;
    }

    public Class<T> messageClass() {
        return clazz;
    }
}
