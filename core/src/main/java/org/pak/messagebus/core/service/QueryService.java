package org.pak.messagebus.core.service;

import org.pak.messagebus.core.Message;
import org.pak.messagebus.core.MessageContainer;
import org.pak.messagebus.core.MessageName;
import org.pak.messagebus.core.SubscriptionName;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

public interface QueryService {
    void initMessageTable(MessageName messageName);
    void initSubscriptionTable(MessageName messageName, SubscriptionName subscriptionName);
    void createMessagePartition(MessageName messageName, Instant includeDateTime);
    void createHistoryPartition(SubscriptionName messageName, Instant includeDateTime);
    List<LocalDate> getAllPartitions(MessageName messageName);
    void dropMessagePartition(MessageName messageName, LocalDate partition);
    void dropHistoryPartition(SubscriptionName messageName, LocalDate partition);
    List<LocalDate> getAllPartitions(SubscriptionName subscriptionName);
    <T> List<MessageContainer<T>> selectMessages(MessageName messageName, SubscriptionName subscriptionName, Integer maxPollRecords);
    <T> void retryMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);
    <T> void failMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer, Exception e);
    <T> void completeMessage(SubscriptionName subscriptionName, MessageContainer<T> messageContainer);
    <T> boolean insertMessage(MessageName messageName, Message<T> message);
    <T> List<Boolean> insertBatchMessage(MessageName messageName, List<Message<T>> messages);
}
