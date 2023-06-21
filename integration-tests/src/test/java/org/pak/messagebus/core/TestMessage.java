package org.pak.messagebus.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestMessage  {
    public static MessageName MESSAGE_NAME = new MessageName("test-message");

    private String name;
}
