package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.pak.messagebus.core.error.ExceptionClassifier;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageBus {
    private final QueryServiceFactory persistenceMessageServiceFactory;
    private final TransactionService transactionService;
    private final ExceptionClassifier exceptionClassifier;

    public MessageBus(
            QueryServiceFactory queryServiceFactory,
            TransactionService transactionService,
            ExceptionClassifier exceptionClassifier
    ) {
        this.persistenceMessageServiceFactory = queryServiceFactory;
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
        var persistenceMessageService = persistenceMessageServiceFactory.createQueryService(
                publisherConfig.getMessageType()
        );

        messagePublishers.computeIfAbsent(publisherConfig.getMessageType(),
                k -> new MessagePublisher<>(publisherConfig.getMessageType(),
                        publisherConfig.getTraceIdExtractor(),
                        persistenceMessageService));
    }

    public <T extends Message> void registerSubscriber(SubscriberConfig<T> subscriberConfig) {
        var persistenceMessageService =
                persistenceMessageServiceFactory.createQueryService(
                        subscriberConfig.getMessageType(),
                        subscriberConfig.getSubscriptionType());

        messageProcessorStarters.computeIfAbsent(
                subscriberConfig.getSubscriptionType(), s -> {
                    var starter = new MessageProcessorStarter<>(subscriberConfig, persistenceMessageService,
                            transactionService, exceptionClassifier);
                    log.info("Register subscriber on message {} with subscription {}",
                            subscriberConfig.getMessageType().name(),
                            subscriberConfig.getSubscriptionType().name());
                    return starter;
                });
    }

    public <T extends Message> void publish(T message) {
        ((MessagePublisher<T>) messagePublishers.get(message.messageType())).publish(message);
    }

    void startSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::start);
    }

    void stopSubscribers() {
        messageProcessorStarters.values().forEach(MessageProcessorStarter::stop);
    }
}
