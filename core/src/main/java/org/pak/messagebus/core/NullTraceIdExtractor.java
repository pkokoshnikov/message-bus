package org.pak.messagebus.core;

public class NullTraceIdExtractor<T extends Message> implements TraceIdExtractor<T>{

    @Override
    public String extractTraceId(T object) {
        return null;
    }
}
