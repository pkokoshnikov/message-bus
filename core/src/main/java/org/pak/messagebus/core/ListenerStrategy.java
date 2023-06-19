package org.pak.messagebus.core;


public interface ListenerStrategy<T> {
    void onStartBatch(int batchSize);

    void handle(MessageContainer<T> message);

    void onEndBatch();
}
