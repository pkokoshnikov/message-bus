package org.pak.messagebus.core;


public class BatchContainerListenerStrategy<T> implements ListenerStrategy<T> {
    private final BatchMessageContainerListener<T> listener;

    public BatchContainerListenerStrategy(BatchMessageContainerListener<T> listener) {this.listener = listener;}


    @Override
    public void onStartBatch(int batchSize) {
        listener.onStartBatch();
    }

    @Override
    public void handle(MessageContainer<T> message) {
        listener.handle(message);
    }

    @Override
    public void onEndBatch() {
        listener.onStartBatch();
    }
}
