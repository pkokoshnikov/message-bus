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

    public MessagePublisher(
            PublisherConfig<T> publisherConfig,
            QueryService queryService,
            MessageFactory messageFactory,
            TableManager tableManager
    ) {
        this.messageName = publisherConfig.getMessageName();
        this.queryService = queryService;
        this.traceIdExtractor = publisherConfig.getTraceIdExtractor();
        this.messageFactory = messageFactory;

        tableManager.registerMessage(messageName, publisherConfig.getProperties().getStorageDays());
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
                    log.warn("Missing partition for {}", message.originatedTime());
                    e.getOriginationTimes().forEach(ot -> queryService.createMessagePartition(messageName, ot));
                    Thread.sleep(50);
                }
            } while (true);
        } catch (InterruptedException e) {
            log.warn("Message publisher is interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
        }
    }
}
