package org.pak.messagebus.core;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.*;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

@Slf4j
class MessageProcessor<T> {

    private final String id = UUID.randomUUID().toString();
    private final ListenerStrategy<T> listenerStrategy;
    private final RetryablePolicy retryablePolicy;
    private final NonRetryablePolicy nonRetryablePolicy;
    private final BlockingPolicy blockingPolicy;
    private final QueryService queryService;
    private final MessageName messageName;
    private final SubscriptionName subscriptionName;
    private final TransactionService transactionService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private Duration pause = null;
    private final Duration unpredictedExceptionPause;
    private final Integer maxPollRecords;
    private final Duration persistenceExceptionPause;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessor(
            @NonNull ListenerStrategy<T> listenerStrategy,
            @NonNull MessageName messageName,
            @NonNull SubscriptionName subscriptionName,
            @NonNull RetryablePolicy retryablePolicy,
            @NonNull NonRetryablePolicy nonRetryablePolicy,
            @NonNull BlockingPolicy blockingPolicy,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull TraceIdExtractor<T> traceIdExtractor,
            @NonNull SubscriberConfig.Properties properties
    ) {
        this.listenerStrategy = listenerStrategy;
        this.retryablePolicy = retryablePolicy;
        this.nonRetryablePolicy = nonRetryablePolicy;
        this.blockingPolicy = blockingPolicy;
        this.queryService = queryService;
        this.messageName = messageName;
        this.subscriptionName = subscriptionName;
        this.transactionService = transactionService;
        this.traceIdExtractor = traceIdExtractor;
        this.maxPollRecords = properties.getMaxPollRecords();
        this.persistenceExceptionPause = properties.getPersistenceExceptionPause();
        this.unpredictedExceptionPause = properties.getUnpredictedExceptionPause();
    }

    public void poolLoop() {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("Event processor should be started only once");
            return;
        }

        try (var ignoreExecutorIdMDC = MDC.putCloseable("messageProcessorId", id);
                var ignoredEventNameMDC = MDC.putCloseable("messageName", messageName.name());
                var ignoredSubscriptionMDC = MDC.putCloseable("subscriptionName", subscriptionName.name())) {
            do {
                log.info("Start pooling");
                try {
                    do {
                        if (pause != null) {
                            log.info("Pause pooling {}", pause);
                            Thread.sleep(pause.toMillis());
                            pause = null;
                        }

                        var isPooled = transactionService.inTransaction(this::poolAndProcess);

                        if (Boolean.FALSE.equals(isPooled)) {
                            Thread.sleep(50);
                        }
                    } while (isRunning.get());
                } catch (InterruptedException e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Event processor is interrupted", e);
                    } else {
                        log.info("Event processor is interrupted");
                    }

                    Thread.currentThread().interrupt();
                    isRunning.set(false);
                } catch (SerializerException e) {
                    log.error("Serializer exception occurred, we cannot skip messages", e);
                    isRunning.set(false);
                } catch (NonRetrayablePersistenceException e) {
                    log.error("Non recoverable persistence exception occurred, stop processing", e);
                    isRunning.set(false);
                } catch (RetrayablePersistenceException e) {
                    log.error("Recoverable persistence exception occurred", e);
                    pause = persistenceExceptionPause;
                } catch (Exception e) {
                    log.error("Unexpected exception occurred", e);
                    pause = unpredictedExceptionPause;
                }
            } while (isRunning.get());

            log.info("Event processor is stopped");
        }
    }

    boolean poolAndProcess() {
        List<MessageContainer<T>> messageContainerList =
                queryService.selectMessages(messageName, subscriptionName, maxPollRecords);

        if (messageContainerList.size() == 0) {
            return false;
        }

        listenerStrategy.onStartBatch(messageContainerList.size());

        for (var messageContainer : messageContainerList) {
            var optionalTraceIdMDC = ofNullable(traceIdExtractor.extractTraceId(messageContainer.getMessage()))
                    .map(v -> MDC.putCloseable("traceId", v));

            try (var ignoreExecutorIdMDC = MDC.putCloseable("messageId", messageContainer.getId().toString());
                    var ignoreKeyMDC = MDC.putCloseable("messageKey", messageContainer.getKey())) {
                try {
                    log.debug("Message handling started");
                    listenerStrategy.handle(messageContainer);

                    queryService.completeMessage(subscriptionName, messageContainer);

                    log.info("Message handling done");
                } catch (PersistenceException e) {
                    throw e;
                } catch (Exception e) {
                    if (blockingPolicy.isBlocked(e)) {
                        handleBlockingException(e);
                    } else if (nonRetryablePolicy.isNonRetryable(e)) {
                        handleNonRetryableException(messageContainer, e);
                    } else {
                        handleRetryableException(messageContainer, e);
                    }
                } finally {
                    optionalTraceIdMDC.ifPresent(MDC.MDCCloseable::close);
                }
            }
        }

        listenerStrategy.onEndBatch();
        return true;
    }

    private void handleNonRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Non retryable exception occurred", e);
        queryService.failMessage(subscriptionName, messageContainer, e);
    }

    private void handleRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Retryable exception occurred, attempt {}", messageContainer.getAttempt(), e);
        var optionalDuration = retryablePolicy.apply(e, messageContainer.getAttempt());

        if (optionalDuration.isEmpty() || messageContainer.getAttempt().equals(Integer.MAX_VALUE - 1)) {
            queryService.failMessage(subscriptionName, messageContainer, e);
        } else {
            queryService.retryMessage(subscriptionName, messageContainer, optionalDuration.get(), e);

            log.debug("Task will retry, attempt {} at {}", messageContainer.getAttempt() + 1,
                    messageContainer.getExecuteAfter());
        }
    }

    private void handleBlockingException(Exception e) {
        log.error("Blocking exception occurred", e);
        pause = blockingPolicy.apply(e);
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Prepare to stop message processor");
        } else {
            log.warn("Event processor should be stopped only once");
        }
    }
}
