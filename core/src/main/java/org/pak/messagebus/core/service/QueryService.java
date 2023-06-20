package org.pak.messagebus.core.service;

import org.apache.commons.lang3.tuple.Triple;
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

    <T> boolean insertMessage(MessageName messageName, MessageDetails<T> messageDetails);
    <T> List<Boolean> insertBatchMessage(MessageName messageName, List<MessageDetails<T>> messages);
}
