package org.pak.messagebus.core;

public interface TraceIdExtractor<T extends Message> {
    String extractTraceId(T object);
}
