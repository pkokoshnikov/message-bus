package org.pak.messagebus.core;

public interface Message {
    MessageType<? extends Message> messageType();
}
