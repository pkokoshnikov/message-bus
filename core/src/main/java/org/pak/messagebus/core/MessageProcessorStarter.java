package org.pak.messagebus.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.ExceptionClassifier;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
class MessageProcessorStarter<T extends Message> {
    private final ExecutorService fixedThreadPoolExecutor;
    private final QueryService<T> persistenceSubscriptionService;
    private final TransactionService transactionService;
    private ExceptionClassifier exceptionClassifier;
    private final SubscriberConfig<T> subscriberConfig;
    private List<MessageProcessor<T>> messageProcessors;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessorStarter(
            SubscriberConfig<T> subscriberConfig,
            QueryService<T> persistenceSubscriptionService,
            TransactionService transactionService,
            ExceptionClassifier exceptionClassifier
    ) {
        this.subscriberConfig = subscriberConfig;
        this.fixedThreadPoolExecutor = Executors.newFixedThreadPool(subscriberConfig.getConcurrency(),
                r -> new ThreadFactoryBuilder()
                        .setNameFormat("message-processor-%d")
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("Uncaught exception in thread {}", t.getName(), e);
                        })
                        .build()
                        .newThread(r));
        this.persistenceSubscriptionService = persistenceSubscriptionService;
        this.transactionService = transactionService;
        this.exceptionClassifier = exceptionClassifier;
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            persistenceSubscriptionService.initMessageTable();
            persistenceSubscriptionService.initSubscriptionTable();

            messageProcessors = IntStream.range(0, subscriberConfig.getConcurrency()).boxed()
                    .map(i -> {
                        var taskExecutor = new MessageProcessor<>(
                                subscriberConfig.getMessageListener(),
                                subscriberConfig.getMessageType(),
                                subscriberConfig.getSubscriptionType(),
                                subscriberConfig.getRetryablePolicy(),
                                subscriberConfig.getBlockingPolicy(),
                                exceptionClassifier,
                                persistenceSubscriptionService,
                                transactionService,
                                subscriberConfig.getTraceIdExtractor(),
                                subscriberConfig.getMaxPollRecords());
                        fixedThreadPoolExecutor.submit(taskExecutor::poolLoop);
                        return taskExecutor;
                    }).toList();
        } else {
            log.warn("Event processor starter should be started only once");
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            messageProcessors.forEach(MessageProcessor::stop);

            fixedThreadPoolExecutor.shutdown();
            try {
                if (fixedThreadPoolExecutor.awaitTermination(30, SECONDS)) {
                    log.info("Event executor stopped");
                } else {
                    log.warn("Event executor did not stop in time");
                    fixedThreadPoolExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Event executor did not stop in time", e);
                Thread.currentThread().interrupt();
            }
        } else {
            log.warn("Event processor starter should be stopped only once");
        }
    }
}
