package org.pak.messagebus.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

@Builder(toBuilder = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
class MessageProcessorFactory<T extends Message> {
    MessageListener<T> messageListener;
    MessageName messageName;
    SubscriptionName subscriptionName;
    RetryablePolicy retryablePolicy;
    BlockingPolicy blockingPolicy;
    NonRetryablePolicy nonRetryablePolicy;
    QueryService queryService;
    TransactionService transactionService;
    TraceIdExtractor<T> traceIdExtractor;
    SubscriberConfig.Properties properties;

    MessageProcessor<T> create() {
        return new MessageProcessor<>(new MessageListenerStrategy<>(messageListener), messageName, subscriptionName,
                retryablePolicy, nonRetryablePolicy, blockingPolicy, queryService, transactionService, traceIdExtractor,
                properties);
    }
}
