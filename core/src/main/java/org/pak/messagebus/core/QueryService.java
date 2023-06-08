package org.pak.messagebus.core;

import org.apache.commons.lang3.StringUtils;
import org.pak.messagebus.core.error.CoreException;

import java.time.Duration;
import java.util.List;

public interface QueryService<T extends Message> {
    void initMessageTable();

    void initSubscriptionTable();

    List<MessageContainer<T>> selectMessages(Integer maxPollRecords);

    void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e);

    void failMessage(MessageContainer<T> messageContainer, Exception e);

    void completeMessage(MessageContainer<T> messageContainer);

    Object insertMessage(T message);

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
