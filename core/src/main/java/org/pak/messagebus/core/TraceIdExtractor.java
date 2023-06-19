package org.pak.messagebus.core;

public interface TraceIdExtractor<T> {
    String extractTraceId(T object);
}
