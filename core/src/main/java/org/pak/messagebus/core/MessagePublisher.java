package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.MissingPartitionException;
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
    private final MessageFactory messageFactory;

    MessagePublisher(
            MessageName messageName,
            TraceIdExtractor<T> traceIdExtractor,
            QueryService queryService,
            MessageFactory messageFactory
    ) {
        this.messageName = messageName;
        this.queryService = queryService;
        this.traceIdExtractor = traceIdExtractor;
        this.messageFactory = messageFactory;

        queryService.initMessageTable(messageName);
    }

    public void publish(T message) {
        publish(messageFactory.createMessage(UUID.randomUUID().toString(), Instant.now(), message));
    }

    //TODO: cleaner
    public void publish(Message<T> message) {
        var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(message.payload()))
                .map(v -> MDC.putCloseable("traceId", v));

        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageName.name());
                var ignoreKeyMDC = MDC.putCloseable("messageKey", message.key())) {
            log.debug("Publish payload {}", message.payload());

            int retryCount = 0;
            do {
                try {
                    var inserted = queryService.insertMessage(messageName, message);
                    if (inserted) {
                        log.info("Published payload");
                    } else {
                        log.warn("Duplicate key {}, {}", message.key(), message.originatedTime());
                    }
                    break;
                } catch (MissingPartitionException e) {
                    e.getOriginationTimes().forEach(ot -> queryService.createMessagePartition(messageName, ot));
                }
            } while (++retryCount < 2);
        } finally {
            optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
        }
    }
}
