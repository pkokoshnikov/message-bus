package org.pak.messagebus.core;

public class NullTraceIdExtractor<T> implements TraceIdExtractor<T>{

    @Override
    public String extractTraceId(Object object) {
        return null;
    }
}
