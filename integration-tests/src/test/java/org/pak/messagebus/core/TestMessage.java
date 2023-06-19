package org.pak.messagebus.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestMessage implements Message {
    public static MessageName MESSAGE_NAME = new MessageName("test-message");

    private String name;

    @Override
    public MessageName messageName() {
        return MESSAGE_NAME;
    }
}
