package org.pak.messagebus.core;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

@Builder
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class PublisherConfig<T extends Message> {
    @NonNull
    MessageType<T> messageType;
    @Builder.Default
    TraceIdExtractor<T> traceIdExtractor = (message) -> null;
}
