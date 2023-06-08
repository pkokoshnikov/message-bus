package org.pak.messagebus.core;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.FieldDefaults;

@Builder
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class SubscriberConfig<T extends Message> {
    @NonNull
    MessageType<T> messageType;
    @NonNull
    SubscriptionType<T> subscriptionType;
    @NonNull
    MessageListener<T> messageListener;
    @Builder.Default
    BlockingPolicy blockingPolicy = new SimpleBlockingPolicy();
    @Builder.Default
    RetryablePolicy retryablePolicy = new SimpleRetryablePolicy();
    @Builder.Default
    int concurrency = 1;
    @Builder.Default
    int maxPollRecords = 1;
    @Builder.Default
    TraceIdExtractor<T> traceIdExtractor = (message) -> null;
}
