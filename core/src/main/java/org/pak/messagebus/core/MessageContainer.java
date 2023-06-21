package org.pak.messagebus.core;

import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.math.BigInteger;
import java.time.Instant;

@Data
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class MessageContainer<T> {
    BigInteger id;
    BigInteger messageId;
    String key;
    T message;
    Integer attempt;
    Instant executeAfter;
    Instant created;
    Instant updated;
    Instant originatedTime;
    String errorMessage;
    String stackTrace;

    public MessageContainer(
            BigInteger id,
            BigInteger messageId,
            String key,
            Integer attempt,
            Instant executeAfter,
            Instant created,
            Instant updated,
            Instant originatedTime,
            T message,
            String errorMessage,
            String stackTrace
    ) {
        this.id = id;
        this.messageId = messageId;
        this.key = key;
        this.attempt = attempt;
        this.executeAfter = executeAfter;
        this.created = created;
        this.updated = updated;
        this.originatedTime = originatedTime;
        this.message = message;
        this.errorMessage = errorMessage;
        this.stackTrace = stackTrace;
    }
}
