package org.pak.messagebus.core;

import java.util.List;

public interface BatchMessageContainerListener<T> {
    void onStartBatch();
    void handle(MessageContainer<T> messageContainer);
    void onEndBatch();
}
