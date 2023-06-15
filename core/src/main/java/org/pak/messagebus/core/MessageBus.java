package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.ExceptionClassifier;
import org.pak.messagebus.core.service.QueryService;
import org.pak.messagebus.core.service.TransactionService;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageBus {
    private final QueryService queryService;
    private final TransactionService transactionService;
    private final ExceptionClassifier exceptionClassifier;

    public MessageBus(
            QueryService queryService,
            TransactionService transactionService,
            ExceptionClassifier exceptionClassifier
    ) {
        this.queryService = queryService;
        this.transactionService = transactionService;
        this.exceptionClassifier = exceptionClassifier;
    }

    private final ConcurrentHashMap<
            SubscriptionType<? extends Message>,
            MessageProcessorStarter<? extends Message>> messageProcessorStarters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<
            MessageType<? extends Message>,
            MessagePublisher<? extends Message>> messagePublishers = new ConcurrentHashMap<>();

    public <T extends Message> void registerPublisher(PublisherConfig<T> publisherConfig) {
        messagePublishers.computeIfAbsent(publisherConfig.getMessageType(),
                k -> new MessagePublisher<>(publisherConfig.getMessageType(),
                        publisherConfig.getTraceIdExtractor(),
                        queryService));
    }

    public <T extends Message> void registerSubscriber(SubscriberConfig<T> subscriberConfig) {
        messageProcessorStarters.computeIfAbsent(
                subscriberConfig.getSubscriptionType(), s -> {
                    var starter = new MessageProcessorStarter<>(subscriberConfig, queryService, transactionService,
                            exceptionClassifier);
                    log.info("Register subscriber on message {} with subscription {}",
                            subscriberConfig.getMessageType().name(),
                            subscriberConfig.getSubscriptionType().name());
                    return starter;
                });
    }

    public <T extends Message> void publish(T message) {
        ((MessagePublisher<T>) messagePublishers.get(message.messageType())).publish(message);
    }

    public void startSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::start);
    }

    public void stopSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::stop);
    }
}
