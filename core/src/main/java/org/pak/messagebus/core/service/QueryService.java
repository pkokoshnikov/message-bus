package org.pak.messagebus.core.service;

import org.pak.messagebus.core.*;
import org.pak.messagebus.core.error.DuplicateKeyException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public interface QueryService {
    void initMessageTable(MessageName messageName);

    void initSubscriptionTable(MessageName messageName, SubscriptionName subscriptionName);

    <T> List<MessageContainer<T>> selectMessages(MessageName messageName, SubscriptionName subscriptionName, Integer maxPollRecords);

    <T> void retryMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);

    <T> void failMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e);

    <T> void completeMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer);

    <T> Object insertMessage(MessageName messageName, String uniqueKey, Instant originatedTime, T message) throws DuplicateKeyException;
}
