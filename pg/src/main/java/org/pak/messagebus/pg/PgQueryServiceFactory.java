package org.pak.messagebus.pg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pak.messagebus.core.*;
import org.pak.messagebus.pg.jsonb.JsonbConverter;

public class PgQueryServiceFactory implements QueryServiceFactory {
    private final PersistenceService persistenceService;
    private final SchemaName schemaName;
    private final JsonbConverter jsonbConverter;

    public PgQueryServiceFactory(PersistenceService persistenceService, SchemaName schemaName, ObjectMapper objectMapper) {
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
        this.jsonbConverter = new JsonbConverter(objectMapper);
    }

    public PgQueryServiceFactory(PersistenceService persistenceService, SchemaName schemaName) {
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
        this.jsonbConverter = new JsonbConverter();
    }

    @Override
    public <T extends Message> QueryService<T> createQueryService(MessageType<T> messageType) {
        jsonbConverter.registerType(messageType.name(), messageType.messageClass());
        return new PgQueryService<>(persistenceService, schemaName, messageType, jsonbConverter);
    }

    @Override
    public <T extends Message> QueryService<T> createQueryService(
            MessageType<T> messageType,
            SubscriptionType<T> subscriptionType
    ) {
        jsonbConverter.registerType(messageType.name(), messageType.messageClass());
        return new PgQueryService<>(persistenceService, schemaName, messageType, subscriptionType, jsonbConverter);
    }
}
