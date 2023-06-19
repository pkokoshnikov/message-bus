package org.pak.messagebus.core;

public class MessageListenerStrategy<T> implements ListenerStrategy<T> {
    private final MessageListener<T> listener;

    public MessageListenerStrategy(MessageListener<T> listener) {this.listener = listener;}

    @Override
    public void onStartBatch(int batchSize) {
        //do nothing
    }

    @Override
    public void handle(MessageContainer<T> messageContainer) {
        listener.handle(messageContainer.getMessage());
    }

    @Override
    public void onEndBatch() {
        //do nothing
    }
}
