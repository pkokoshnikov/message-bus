package org.pak.messagebus.core;

public interface MessageListener<T extends Message> {
    void handle(T task);
}
