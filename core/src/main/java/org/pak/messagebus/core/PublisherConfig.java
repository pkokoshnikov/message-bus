package org.pak.messagebus.core;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

@Builder
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class PublisherConfig<T> {
    @NonNull
    MessageName messageName;
    @NonNull
    Class<T> clazz;
    @Builder.Default
    TraceIdExtractor<T> traceIdExtractor = new NullTraceIdExtractor<>();
}
