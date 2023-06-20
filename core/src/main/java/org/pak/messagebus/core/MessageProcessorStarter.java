package org.pak.messagebus.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
class MessageProcessorStarter<T> {
    private final ExecutorService fixedThreadPoolExecutor;
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final SubscriberConfig<T> subscriberConfig;
    private final int concurrency;
    private List<MessageProcessor<T>> messageProcessors;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    MessageProcessorStarter(
            SubscriberConfig<T> subscriberConfig,
            QueryService queryService,
            TransactionService transactionService
    ) {
        this.subscriberConfig = subscriberConfig;
        concurrency = subscriberConfig.getProperties().getConcurrency();

        this.fixedThreadPoolExecutor = Executors.newFixedThreadPool(concurrency,
                r -> new ThreadFactoryBuilder()
                        .setNameFormat("message-processor-%d")
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("Uncaught exception in thread {}", t.getName(), e);
                        })
                        .build()
                        .newThread(r));
        this.queryService = queryService;
        this.transactionService = transactionService;
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            queryService.initMessageTable(subscriberConfig.getMessageName());
            queryService.initSubscriptionTable(subscriberConfig.getMessageName(),
                    subscriberConfig.getSubscriptionName());

            messageProcessors = IntStream.range(0, concurrency).boxed()
                    .map(i -> {
                        var taskExecutor = new MessageProcessor<>(
                                selectListenerStrategy(subscriberConfig),
                                subscriberConfig.getMessageName(),
                                subscriberConfig.getSubscriptionName(),
                                subscriberConfig.getRetryablePolicy(),
                                subscriberConfig.getNonRetryablePolicy(),
                                subscriberConfig.getBlockingPolicy(),
                                queryService,
                                transactionService,
                                subscriberConfig.getTraceIdExtractor(),
                                subscriberConfig.getProperties());
                        fixedThreadPoolExecutor.submit(taskExecutor::poolLoop);
                        return taskExecutor;
                    }).toList();
        } else {
            log.warn("Event processor starter should be started only once");
        }
    }

    private ListenerStrategy<T> selectListenerStrategy(SubscriberConfig<T> subscriberConfig) {
        if (subscriberConfig.getMessageListener() != null) {
            return new MessageListenerStrategy<>(subscriberConfig.getMessageListener());
        } else if (subscriberConfig.getBatchMessageContainerListener() != null) {
            return new BatchContainerListenerStrategy<>(subscriberConfig.getBatchMessageContainerListener());
        } else {
            throw new IllegalStateException("Message listener or batch message listener should be set");
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
