package org.pak.messagebus.core;

public interface MessageListener<T> {
    void handle(Message<T> message);
}
