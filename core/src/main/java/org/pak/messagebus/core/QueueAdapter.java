package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QueueAdapter {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final MessageFactory messageFactory;
    private final ConcurrentHashMap<String, MessageProcessorStarter<?>> messageProcessorStarters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, QueueMessagePublisher<?>> messagePublishers =
            new ConcurrentHashMap<>();

    public QueueAdapter(
            QueryService queryService,
            TransactionService transactionService,
            MessageFactory messageFactory
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.messageFactory = messageFactory;
    }

    public <T> void registerPublisher(PublisherConfig<T> publisherConfig) {
        messagePublishers.computeIfAbsent(publisherConfig.getClazz(),
                k -> new QueueMessagePublisher<>(publisherConfig.getMessageName(),
                        publisherConfig.getTraceIdExtractor(),
                        queryService, transactionService));
    }

    public <T> void registerSubscriber(SubscriberConfig<T> subscriberConfig) {
        messageProcessorStarters.computeIfAbsent(
                subscriberConfig.getMessageName() + "_" + subscriberConfig.getSubscriptionName(), s -> {
                    var starter = new MessageProcessorStarter<>(subscriberConfig, queryService, transactionService,
                            messageFactory);
                    log.info("Register subscriber on payload {} with subscription {}",
                            subscriberConfig.getMessageName(),
                            subscriberConfig.getSubscriptionName());
                    return starter;
                });
    }

    public <T> void publish(Class<T> clazz, List<Message<T>> messages) {
        ((QueueMessagePublisher<T>) messagePublishers.get(clazz)).publish(messages);
    }

    public void startSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::start);
    }

    public void stopSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::stop);
    }

}
