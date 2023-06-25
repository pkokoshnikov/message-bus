package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.service.QueryService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class MessagePublisherFactory<T> {
    MessageName messageName;
    TraceIdExtractor<T> traceIdExtractor;
    QueryService queryService;
    MessageFactory messageFactory;

    MessagePublisher<T> create() {
        return new MessagePublisher<>(messageName, traceIdExtractor, queryService, messageFactory);
    }
}
