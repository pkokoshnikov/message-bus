package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.DuplicateKeyException;
import org.pak.messagebus.core.service.QueryService;
import org.slf4j.MDC;

import java.util.Optional;
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
        publish(UUID.randomUUID().toString(), message);
    }

    //TODO:add create time
    //TODO: add message types: COMMAND, EVENT, QUERY, REPLY
    public void publish(String uniqueKey, T message) {
        var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(message))
                .map(v -> MDC.putCloseable("traceId", v));
        var optionalMessageIdMDC = Optional.<MDC.MDCCloseable>empty();

        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageName.name());
                var ignoreKeyMDC = MDC.putCloseable("messageKey", uniqueKey)) {
            log.debug("Publish message {}", message);

            var messageId = queryService.insertMessage(messageName, uniqueKey, message);

            optionalMessageIdMDC = ofNullable(messageId).map(v -> MDC.putCloseable("messageId", v.toString()));

            log.info("Published message");
        } catch (DuplicateKeyException e) {
            if (log.isDebugEnabled()) {
                log.warn("Duplicate key {} exception, {}", uniqueKey, message);
            } else {
                log.warn("Duplicate key {} exception", uniqueKey);
            }
        } finally {
            optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
            optionalMessageIdMDC.ifPresent(MDC.MDCCloseable::close);
        }
    }
}
