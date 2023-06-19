package org.pak.messagebus.core;

public class EmptyMessageContainerListener<T> implements MessageContainerListener<T>{
    @Override
    public void handle(MessageContainer<T> message) {

    }
}
