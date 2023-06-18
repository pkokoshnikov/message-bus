package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.error.ExceptionClassifier;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class MessageProcessorFactory<T extends Message> {
    MessageListener<T> messageListener;
    MessageType<T> messageType;
    SubscriptionType<T> subscriptionType;
    RetryablePolicy retryablePolicy;
    BlockingPolicy blockingPolicy;
    ExceptionClassifier exceptionClassifier;
    QueryService queryService;
    TransactionService transactionService;
    TraceIdExtractor<T> traceIdExtractor;
    Integer maxPollRecords;

    MessageProcessor<T> create() {
        return new MessageProcessor<>(messageListener, messageType, subscriptionType, retryablePolicy,
                blockingPolicy, exceptionClassifier, queryService, transactionService, traceIdExtractor,
                maxPollRecords);
    }
}
