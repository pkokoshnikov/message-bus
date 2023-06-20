package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageBus {
    private final QueryService queryService;
    private final TransactionService transactionService;

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
    }

    private final ConcurrentHashMap<String, MessageProcessorStarter<?>> messageProcessorStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, MessagePublisher<?>> messagePublishers =
            new ConcurrentHashMap<>();

    public <T> void registerPublisher(PublisherConfig<T> publisherConfig) {
        messagePublishers.computeIfAbsent(publisherConfig.getClazz(),
                k -> new MessagePublisher<>(publisherConfig.getMessageName(),
                        publisherConfig.getTraceIdExtractor(),
                        queryService));
    }

    public <T extends Message> void registerSubscriber(SubscriberConfig<T> subscriberConfig) {
        messageProcessorStarters.computeIfAbsent(
                subscriberConfig.getMessageName() + "_" + subscriberConfig.getSubscriptionName(), s -> {
                    var starter = new MessageProcessorStarter<>(subscriberConfig, queryService, transactionService);
                    log.info("Register subscriber on message {} with subscription {}",
                            subscriberConfig.getMessageName(),
                            subscriberConfig.getSubscriptionName());
                    return starter;
                });
    }

    public <T> void publish(T message) {
        ((MessagePublisher<T>) messagePublishers.get(message.getClass())).publish(message);
    }

    public void startSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::start);
    }

    public void stopSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::stop);
    }
}
