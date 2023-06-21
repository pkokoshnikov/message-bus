package org.pak.messagebus.core.service;

import org.pak.messagebus.core.*;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    void initMessageTable(MessageName messageName);

    void initSubscriptionTable(MessageName messageName, SubscriptionName subscriptionName);

    <T> List<MessageContainer<T>> selectMessages(MessageName messageName, SubscriptionName subscriptionName, Integer maxPollRecords);

    <T> void retryMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);

    <T> void failMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e);

    <T> void completeMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer);

    <T> boolean insertMessage(MessageName messageName, Message<T> message);
    <T> List<Boolean> insertBatchMessage(MessageName messageName, List<Message<T>> messages);
}
