package org.pak.messagebus.core.service;

import org.apache.commons.lang3.StringUtils;
import org.pak.messagebus.core.MessageContainer;
import org.pak.messagebus.core.Message;
import org.pak.messagebus.core.MessageType;
import org.pak.messagebus.core.SubscriptionType;
import org.pak.messagebus.core.error.CoreException;

import java.time.Duration;
import java.util.List;

public interface QueryService {
    <T extends Message> void initMessageTable(MessageType<T> messageType);

    <T extends Message> void initSubscriptionTable(MessageType<T> messageType, SubscriptionType<T> subscriptionType);

    <T extends Message> List<MessageContainer<T>> selectMessages(MessageType<T> messageType, SubscriptionType<T> subscriptionType, Integer maxPollRecords);

    <T extends Message> void retryMessage(SubscriptionType<T> subscriptionType, MessageContainer<T> messageContainer, Duration retryDuration, Exception e);

    <T extends Message> void failMessage(SubscriptionType<T> subscriptionType, MessageContainer<T> messageContainer, Exception e);

    <T extends Message> void completeMessage(SubscriptionType<T> subscriptionType, MessageContainer<T> messageContainer);

    <T extends Message> Object insertMessage(MessageType<T> messageType, T message);

    default void assertNonEmptyUpdate(int updated, String query, Object... args) {
        if (updated == 0) {
            throw new CoreException(
                    "Incorrect state, no records were updated by query %s for args %s".formatted(
                            query,
                            StringUtils.joinWith(", ", args))
            );
        }
    }
}
