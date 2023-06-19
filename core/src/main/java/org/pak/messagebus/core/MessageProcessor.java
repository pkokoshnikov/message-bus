package org.pak.messagebus.core;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.*;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.pak.messagebus.core.error.ExceptionType.RETRYABLE;

@Slf4j
class MessageProcessor<T> {
    public static final Duration DEFAULT_BLOCKING_DURATION = Duration.of(30, ChronoUnit.SECONDS);
    private final String id = UUID.randomUUID().toString();
    private final ListenerStrategy<T> listenerStrategy;
    private final RetryablePolicy retryablePolicy;
    private final BlockingPolicy blockingPolicy;
    private final ExceptionClassifier exceptionClassifier;
    private final QueryService queryService;
    private final MessageName messageName;
    private final SubscriptionName subscriptionName;
    private final TransactionService transactionService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private Duration pause = null;
    private final Integer maxPollRecords;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessor(
            @NonNull ListenerStrategy<T> listenerStrategy,
            @NonNull MessageName messageName,
            @NonNull SubscriptionName subscriptionName,
            @NonNull RetryablePolicy retryablePolicy,
            @NonNull BlockingPolicy blockingPolicy,
            @NonNull ExceptionClassifier exceptionClassifier,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull TraceIdExtractor<T> traceIdExtractor,
            @NonNull Integer maxPollRecords
    ) {
        this.listenerStrategy = listenerStrategy;
        this.retryablePolicy = retryablePolicy;
        this.blockingPolicy = blockingPolicy;
        this.exceptionClassifier = exceptionClassifier;
        this.queryService = queryService;
        this.messageName = messageName;
        this.subscriptionName = subscriptionName;
        this.transactionService = transactionService;
        this.traceIdExtractor = traceIdExtractor;
        this.maxPollRecords = maxPollRecords;
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
                try {
                    do {
                        if (pause != null) {
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
                } catch (Exception e) {
                    handleBlockingException(e);
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
                } catch (IncorrectStateException e) {
                    log.error("Incorrect state of processing", e);
                } catch (PersistenceException e) {
                    throw e;
                } catch (Exception e) {
                    if (exceptionClassifier.isBlockedException(e)) {
                        handleBlockingException(e);
                    } else if (exceptionClassifier.isNonRetryableException(e)) {
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

        if (pause == null) {
            log.warn("Blocking policy configured incorrectly, duration cannot be null");
            pause = DEFAULT_BLOCKING_DURATION;
        }

        log.info("Block message processor until {}", pause);
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("Prepare to stop message processor");
        } else {
            log.warn("Event processor should be stopped only once");
        }
    }
}
