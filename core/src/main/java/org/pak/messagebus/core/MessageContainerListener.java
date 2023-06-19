package org.pak.messagebus.core;

public interface MessageContainerListener<T> {
    void handle(MessageContainer<T> message);

}
