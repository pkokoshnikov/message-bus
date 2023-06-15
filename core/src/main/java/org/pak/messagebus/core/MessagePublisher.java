package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.slf4j.MDC;

import static java.util.Optional.ofNullable;

@Slf4j
class MessagePublisher<T extends Message> {
    private final MessageType<T> messageType;
    private final QueryService queryService;
    private final TraceIdExtractor<T> traceIdExtractor;

    MessagePublisher(
            MessageType<T> messageType,
            TraceIdExtractor<T> traceIdExtractor,
            QueryService queryService
    ) {
        this.messageType = messageType;
        this.queryService = queryService;
        this.traceIdExtractor = traceIdExtractor;

        queryService.initMessageTable(messageType);
    }

    public void publish(T message) {
        var optionalTraceId = ofNullable(traceIdExtractor.extractTraceId(message))
                .map(v -> MDC.putCloseable("traceId", v));

        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageType.name())) {
            log.debug("Publish message {}", message);

            ofNullable(queryService.insertMessage(messageType, message))
                    .ifPresentOrElse(v -> {
                        try (var ignoreExecutorId = MDC.putCloseable("messageId", v.toString())) {
                            log.info("Published message");
                        }
                    }, () -> log.info("Published message"));
        } finally {
            optionalTraceId.ifPresent(MDC.MDCCloseable::close);
        }
    }
}
