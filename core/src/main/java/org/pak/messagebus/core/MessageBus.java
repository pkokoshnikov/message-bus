package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageBus {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final MessageFactory messageFactory;
    private final TableManager tableManager;

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory,
            CronConfig cronConfig
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = messageFactory;
        this.tableManager = new TableManager(queryService, cronConfig.getCreatingPartitionsCron(),
                cronConfig.getCleaningPartitionsCron());
    }

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService,
            CronConfig cronConfig
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = new StdMessageFactory();
        this.tableManager = new TableManager(queryService, cronConfig.getCreatingPartitionsCron(),
                cronConfig.getCleaningPartitionsCron());
    }

    private final ConcurrentHashMap<String, MessageProcessorStarter<?>> messageProcessorStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, MessagePublisher<?>> messagePublishers =
            new ConcurrentHashMap<>();

    public <T> void registerPublisher(PublisherConfig<T> publisherConfig) {
        messagePublishers.computeIfAbsent(publisherConfig.getClazz(),
                k -> new MessagePublisher<>(publisherConfig.getMessageName(),
                        publisherConfig.getTraceIdExtractor(),
                        queryService,
                        messageFactory));

        tableManager.registerMessage(publisherConfig.getMessageName(),
                publisherConfig.getProperties().getStorageDays());
    }

    public <T> void registerSubscriber(SubscriberConfig<T> subscriberConfig) {
        messageProcessorStarters.computeIfAbsent(
                subscriberConfig.getMessageName() + "_" + subscriberConfig.getSubscriptionName(), s -> {
                    var starter = new MessageProcessorStarter<>(subscriberConfig, queryService, transactionService,
                            messageFactory, tableManager);
                    log.info("Register subscriber on payload {} with subscription {}",
                            subscriberConfig.getMessageName(),
                            subscriberConfig.getSubscriptionName());
                    return starter;
                });

        tableManager.registerSubscription(subscriberConfig.getMessageName(), subscriberConfig.getSubscriptionName(),
                subscriberConfig.getProperties().getStorageDays());
    }

    public <T> void publish(T message) {
        ((MessagePublisher<T>) messagePublishers.get(message.getClass())).publish(message);
    }

    public void startTableManager() {
        tableManager.startCronJobs();
    }

    public void stopTableManager() {
        tableManager.stopCronJobs();
    }

    public void startSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::start);
    }

    public void stopSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::stop);
    }
}
