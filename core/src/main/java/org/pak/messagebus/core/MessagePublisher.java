package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.UUID;

import static java.util.Optional.ofNullable;

@Slf4j
class MessagePublisher<T> {
    private final MessageName messageName;
    private final QueryService queryService;
    private final TraceIdExtractor<T> traceIdExtractor;

    MessagePublisher(
            MessageName messageName,
            TraceIdExtractor<T> traceIdExtractor,
            QueryService queryService
    ) {
        this.messageName = messageName;
        this.queryService = queryService;
        this.traceIdExtractor = traceIdExtractor;

        queryService.initMessageTable(messageName);
    }

    public void publish(T message) {
        publish(new MessageDetails<>(UUID.randomUUID().toString(), Instant.now(), message));
    }

    //TODO: add create time
    //TODO: add message types: COMMAND, EVENT, QUERY, REPLY
    //TODO: cleaner
    //TODO: partitioning
    public void publish(MessageDetails<T> messageDetails) {
        var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(messageDetails.getMessage()))
                .map(v -> MDC.putCloseable("traceId", v));

        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageName.name());
                var ignoreKeyMDC = MDC.putCloseable("messageKey", messageDetails.getKey())) {
            log.debug("Publish message {}", messageDetails.getMessage());

            var inserted = queryService.insertMessage(messageName, messageDetails);

            if (inserted) {
                log.info("Published message");
            } else {
                log.warn("Duplicate key {}, {}", messageDetails.getKey(), messageDetails.getOriginatedTime());
            }
        } finally {
            optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
        }
    }
}
