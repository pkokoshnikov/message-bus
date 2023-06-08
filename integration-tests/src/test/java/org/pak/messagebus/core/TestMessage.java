package org.pak.messagebus.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestMessage implements Message {
    public static MessageType<TestMessage> MESSAGE_TYPE = new MessageType<>("test-message", TestMessage.class);
    private String name;

    @Override
    public MessageType<TestMessage> messageType() {
        return MESSAGE_TYPE;
    }
}
