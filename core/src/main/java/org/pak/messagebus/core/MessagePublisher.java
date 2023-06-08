package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
class MessagePublisher<T extends Message> {
    private final MessageType<T> messageType;
    private final QueryService<T> queryService;
    private final TraceIdExtractor<T> traceIdExtractor;

    MessagePublisher(
            MessageType<T> messageType,
            TraceIdExtractor<T> traceIdExtractor,
            QueryService<T> queryService
    ) {
        this.messageType = messageType;
        this.queryService = queryService;
        this.traceIdExtractor = traceIdExtractor;

        queryService.initMessageTable();
    }

    public void publish(T message) {
        try (var ignoredTraceIdMDC = MDC.putCloseable("traceId", traceIdExtractor.extractTraceId(message));
                var ignoredCollectionMDC = MDC.putCloseable("messageName", messageType.name())) {
            log.debug("Publish message {}", message);

            var messageId = queryService.insertMessage(message);

            if (messageId == null) {
                log.info("Published message");
            } else {
                try (var ignoreExecutorId = MDC.putCloseable("messageId", messageId.toString())) {
                    log.info("Published message");
                }
            }
        }
    }
}
