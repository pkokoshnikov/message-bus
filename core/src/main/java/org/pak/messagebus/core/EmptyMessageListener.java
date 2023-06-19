package org.pak.messagebus.core;

public class EmptyMessageListener<T> implements MessageListener<T>{
    @Override
    public void handle(T message) {

    }
}
