package org.pak.messagebus.core;

public interface QueryServiceFactory {
    <T extends Message> QueryService<T> createQueryService(MessageType<T> messageType);
    <T extends Message> QueryService<T> createQueryService(MessageType<T> messageType, SubscriptionType<T> subscriptionType);
}
