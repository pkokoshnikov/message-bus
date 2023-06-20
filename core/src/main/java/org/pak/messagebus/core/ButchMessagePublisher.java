package org.pak.messagebus.core;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.util.Optional.ofNullable;

@Slf4j
class ButchMessagePublisher<T> {
    private final MessageName messageName;
    private final QueryService queryService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private final TransactionService transactionService;

    ButchMessagePublisher(
            MessageName messageName,
            TraceIdExtractor<T> traceIdExtractor,
            QueryService queryService,
            TransactionService transactionService
    ) {
        this.messageName = messageName;
        this.queryService = queryService;
        this.traceIdExtractor = traceIdExtractor;
        this.transactionService = transactionService;

        queryService.initMessageTable(messageName);
    }

    public void publishMessages(List<T> messages) {
        publishDetailedMessages(messages.stream()
                .map(message -> new MessageDetails<>(UUID.randomUUID().toString(), Instant.now(), message))
                .toList());
    }

    public void publishDetailedMessages(List<MessageDetails<T>> messages) {
        try (var ignoredCollectionMDC = MDC.putCloseable("messageName", messageName.name())) {
            if (log.isDebugEnabled()) {
                messages.forEach(messageDetails -> {
                    var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(messageDetails.getMessage()))
                            .map(v -> MDC.putCloseable("traceId", v));

                    log.debug("Publish message {}", messageDetails);

                    optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
                });
            }

            var inserted = transactionService.inTransaction(() ->
                    queryService.insertBatchMessage(messageName, messages));

            for (int i = 0; i < inserted.size(); i++) {
                var message = messages.get(i);
                var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(message.getMessage()))
                        .map(v -> MDC.putCloseable("traceId", v));
                try (var ignoreKeyMDC = MDC.putCloseable("messageKey", message.getKey())) {
                    if (inserted.get(i)) {
                        log.info("Published message");
                    } else {
                        log.warn("Duplicate key {}, {}", message.getKey(), message.getOriginatedTime());
                    }
                } finally {
                    optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
                }
            }
        }
    }
}
