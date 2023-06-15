package org.pak.messagebus.core;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.CoreException;
import org.pak.messagebus.core.error.ExceptionClassifier;
import org.pak.messagebus.core.error.ExceptionType;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.ofNullable;
import static org.pak.messagebus.core.error.ExceptionType.RETRYABLE;

@Slf4j
class MessageProcessor<T extends Message> {
    private static final Duration DEFAULT_BLOCKING_DURATION = Duration.of(30, ChronoUnit.SECONDS);
    private final String id = UUID.randomUUID().toString();
    private final MessageListener<T> messageListener;
    private final RetryablePolicy retryablePolicy;
    private final BlockingPolicy blockingPolicy;
    private final ExceptionClassifier exceptionClassifier;
    private final QueryService queryService;
    private final MessageType<T> messageType;
    private final SubscriptionType<T> subscriptionType;
    private final TransactionService transactionService;
    private final TraceIdExtractor<T> traceIdExtractor;
    private Duration pause = null;
    private final Integer maxPollRecords;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessor(
            @NonNull MessageListener<T> messageListener,
            @NonNull MessageType<T> messageType,
            @NonNull SubscriptionType<T> subscriptionType,
            @NonNull RetryablePolicy retryablePolicy,
            @NonNull BlockingPolicy blockingPolicy,
            @NonNull ExceptionClassifier exceptionClassifier,
            @NonNull QueryService queryService,
            @NonNull TransactionService transactionService,
            @NonNull TraceIdExtractor<T> traceIdExtractor,
            @NonNull Integer maxPollRecords
    ) {
        this.messageListener = messageListener;
        this.retryablePolicy = retryablePolicy;
        this.blockingPolicy = blockingPolicy;
        this.exceptionClassifier = exceptionClassifier;
        this.queryService = queryService;
        this.messageType = messageType;
        this.subscriptionType = subscriptionType;
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
                var ignoredEventNameMDC = MDC.putCloseable("messageName", messageType.name());
                var ignoredSubscriptionMDC = MDC.putCloseable("subscriptionName", subscriptionType.name())) {
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
                } catch (Exception e) {
                    if (ofNullable(exceptionClassifier.classify(e)).orElse(RETRYABLE) == ExceptionType.BLOCKING) {
                        handleBlockingException(e);
                    } else {
                        log.error("Unpredicted exception is occurred", e);
                        pause = DEFAULT_BLOCKING_DURATION;
                    }
                }
            } while (isRunning.get());

            log.info("Event processor is stopped");
        }
    }

    boolean poolAndProcess() {
        List<MessageContainer<T>> messageContainerList =
                queryService.selectMessages(messageType, subscriptionType, maxPollRecords);

        if (messageContainerList.size() == 0) {
            return false;
        }

        for (var messageContainer : messageContainerList) {
            var optionalTraceId = ofNullable(traceIdExtractor.extractTraceId(messageContainer.getMessage()))
                    .map(v -> MDC.putCloseable("traceId", v));

            try (var ignoreExecutorId = MDC.putCloseable("messageId", messageContainer.getId().toString())) {
                try {
                    log.debug("Message handling started");
                    messageListener.handle(messageContainer.getMessage());

                    queryService.completeMessage(subscriptionType, messageContainer);

                    log.info("Message handling done");
                } catch (CoreException e) {
                    log.error("Core exception occurred", e);
                    queryService.failMessage(subscriptionType, messageContainer, e);
                } catch (Exception e) {
                    switch (ofNullable(exceptionClassifier.classify(e)).orElse(RETRYABLE)) {
                        case NON_RETRYABLE -> handleNonRetryableException(messageContainer, e);
                        case BLOCKING -> handleBlockingException(e);
                        case RETRYABLE -> handleRetryableException(messageContainer, e);
                        default -> throw new CoreException("Unexpected value: " + exceptionClassifier.classify(e));
                    }
                } finally {
                    optionalTraceId.ifPresent(MDC.MDCCloseable::close);
                }
            }
        }

        return true;
    }

    private void handleNonRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Non retryable exception occurred", e);
        queryService.failMessage(subscriptionType, messageContainer, e);
    }

    private void handleRetryableException(MessageContainer<T> messageContainer, Exception e) {
        log.error("Exception occurred, attempt {}", messageContainer.getAttempt(), e);
        var retryDuration = retryablePolicy.apply(e, messageContainer.getAttempt());

        if (retryDuration == null || messageContainer.getAttempt().equals(Integer.MAX_VALUE - 1)) {
            queryService.failMessage(subscriptionType, messageContainer, e);
        } else {
            queryService.retryMessage(subscriptionType, messageContainer, retryDuration, e);

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
