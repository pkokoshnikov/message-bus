package org.pak.messagebus.pg;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.pak.messagebus.core.*;
import org.pak.messagebus.core.error.CoreException;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.postgresql.util.PGobject;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;

@Slf4j
public class PgQueryService<T extends Message> implements QueryService<T> {
    private final PersistenceService persistenceService;
    private final SchemaName schemaName;
    private final String messageTable;
    private final String subscriptionTable;
    private final String subscriptionHistoryTable;
    private final JsonbConverter jsonbConverter;

    public PgQueryService(
            PersistenceService persistenceService,
            SchemaName schemaName,
            MessageType<T> messageType,
            SubscriptionType<T> subscriptionType,
            JsonbConverter jsonbConverter
    ) {
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
        this.jsonbConverter = jsonbConverter;
        this.messageTable = messageType.name().replace("-", "_");
        this.subscriptionTable = subscriptionType.name().replace("-", "_");
        this.subscriptionHistoryTable = subscriptionType.name().replace("-", "_") + "_history";
    }

    public PgQueryService(
            PersistenceService persistenceService,
            SchemaName schemaName,
            MessageType<T> messageType,
            JsonbConverter jsonbConverter
    ) {
        this.persistenceService = persistenceService;
        this.schemaName = schemaName;
        this.messageTable = messageType.name().replace("-", "_");
        this.jsonbConverter = jsonbConverter;
        this.subscriptionTable = null;
        this.subscriptionHistoryTable = null;
    }

    @Override
    public void initMessageTable() {
        var query = Utils.format("""
                CREATE TABLE IF NOT EXISTS ${schema}.${messageTable} (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    payload JSONB NOT NULL)
                """, Map.of("schema", schemaName.value(),
                "messageTable", messageTable));

        persistenceService.execute(query);
    }


    public void initSubscriptionTable() {
        if (subscriptionTable == null) {
            throw new IllegalArgumentException("This instance is not configured to use subscription table");
        }

        var query = Utils.format("""
                        CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionTable} (
                            id BIGSERIAL PRIMARY KEY,
                            message_id BIGINT NOT NULL REFERENCES ${messageTable}(id),
                            attempt INTEGER NOT NULL DEFAULT 0,
                            error_message TEXT,
                            stack_trace TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE,
                            execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP);
                                                
                        CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionTable}_message_id_idx ON ${schema}.${subscriptionTable}(message_id);
                        CREATE INDEX IF NOT EXISTS ${subscriptionTable}_created_at_idx ON ${schema}.${subscriptionTable}(created_at);
                        CREATE INDEX IF NOT EXISTS ${subscriptionTable}_execute_after_idx ON ${schema}.${subscriptionTable}(execute_after ASC);
                                                
                        CREATE TABLE IF NOT EXISTS ${schema}.${subscriptionHistoryTable} (
                            id BIGINT PRIMARY KEY,
                            message_id BIGINT NOT NULL REFERENCES ${messageTable}(id),
                            attempt INTEGER NOT NULL DEFAULT 0,
                            status TEXT NOT NULL DEFAULT 'PROCESSED',
                            error_message TEXT,
                            stack_trace TEXT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP);
                            
                        CREATE UNIQUE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_message_id_idx ON ${schema}.${subscriptionHistoryTable}(message_id);
                        CREATE INDEX IF NOT EXISTS ${subscriptionHistoryTable}_created_at_idx ON ${schema}.${subscriptionHistoryTable}(created_at);
                                        
                        CREATE OR REPLACE FUNCTION ${schema}.${insertFunction}
                          RETURNS trigger AS
                        $$
                            BEGIN
                            INSERT INTO ${schema}.${subscriptionTable}(message_id, created_at, execute_after)
                                 VALUES(NEW.id, NEW.created_at, NEW.execute_after);
                            RETURN NEW;
                            END;
                        $$
                        LANGUAGE 'plpgsql';
                            
                        CREATE OR REPLACE TRIGGER ${insertTrigger}
                            AFTER INSERT ON ${schema}.${messageTable}
                            FOR EACH ROW
                            EXECUTE PROCEDURE ${schema}.${insertFunction};
                        """,
                Map.of("schema", schemaName.value(),
                        "messageTable", messageTable,
                        "subscriptionTable", subscriptionTable,
                        "subscriptionHistoryTable", subscriptionHistoryTable,
                        "insertTrigger", subscriptionTable.replace("-", "_") + "_insert_trigger",
                        "insertFunction", subscriptionTable.replace("-", "_") + "_insert_function()"));

        persistenceService.execute(query);
    }

    @Override
    public Object insertMessage(T message) {
        String query = Utils.format("""
                        INSERT INTO ${schema}.${messageTable} (created_at, execute_after, payload)
                        VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)""",
                Map.of("schema", schemaName.value(),
                        "messageTable", messageTable));

        return persistenceService.insert(query, jsonbConverter.toPGObject(message));
    }

    public List<MessageContainer<T>> selectMessages(Integer maxPollRecords) {
        if (subscriptionTable == null) {
            throw new IllegalArgumentException("This instance is not configured to use subscription table");
        }

        var query = Utils.format("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, e.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${messageTable} e ON s.message_id = e.id
                        WHERE s.execute_after < CURRENT_TIMESTAMP
                        ORDER BY s.execute_after ASC
                        LIMIT ${maxPollRecords} FOR UPDATE OF s SKIP LOCKED""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable,
                        "messageTable", messageTable,
                        "maxPollRecords", maxPollRecords.toString()));

        return persistenceService.query(query, rs -> {
            try {
                return new MessageContainer<>(rs.getObject("id", BigInteger.class),
                        rs.getObject("message_id", BigInteger.class),
                        rs.getInt("attempt"),
                        ofNullable(rs.getObject("execute_after", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("created_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("updated_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        jsonbConverter.toJsonb(rs.getObject("payload", PGobject.class)),
                        rs.getString("error_message"),
                        rs.getString("stack_trace"));
            } catch (SQLException e) {
                throw new CoreException(e);
            }
        });
    }

    public void retryMessage(MessageContainer<T> messageContainer, Duration retryDuration, Exception e) {
        if (subscriptionTable == null) {
            throw new CoreException("This instance is not configured to use subscription table");
        }

        var query = Utils.format("""
                        UPDATE ${schema}.${subscriptionTable} SET updated_at = CURRENT_TIMESTAMP,
                            execute_after = CURRENT_TIMESTAMP + interval '${retryDuration} seconds', attempt = attempt + 1,
                            error_message = ?, stack_trace = ?
                        WHERE id = ?""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable,
                        "retryDuration", String.valueOf(retryDuration.getSeconds())));

        var updated = persistenceService.update(query, e.getMessage(), ExceptionUtils.getStackTrace(e),
                messageContainer.getId());

        assertNonEmptyUpdate(updated, query, messageContainer.getId());
    }

    public void failMessage(MessageContainer<T> messageContainer, Exception e) {
        if (subscriptionTable == null) {
            throw new CoreException("This instance is not configured to use subscription table");
        }

        var query = Utils.format("""
                        WITH deleted AS (DELETE FROM ${schema}.${subscriptionTable} WHERE id = ? RETURNING *)
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, attempt, 'FAILED' as status, ?, ? FROM deleted""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable,
                        "subscriptionHistoryTable", subscriptionHistoryTable));

        var updated = persistenceService.update(query, messageContainer.getId(), e.getMessage(),
                ExceptionUtils.getStackTrace(e));

        assertNonEmptyUpdate(updated, query, messageContainer.getId());
    }

    public void completeMessage(MessageContainer<T> messageContainer) {
        if (subscriptionTable == null) {
            throw new CoreException("This instance is not configured to use subscription table");
        }

        var query = Utils.format("""
                        WITH deleted AS (DELETE FROM ${schema}.${subscriptionTable} WHERE id = ? RETURNING *)
                        INSERT INTO ${schema}.${subscriptionHistoryTable}
                            (id, message_id, attempt, status, error_message, stack_trace)
                            SELECT id, message_id, attempt, 'PROCESSED' as status, error_message, stack_trace FROM deleted""",
                Map.of("schema", schemaName.value(),
                        "subscriptionTable", subscriptionTable,
                        "subscriptionHistoryTable", subscriptionHistoryTable));

        persistenceService.update(query, messageContainer.getId());
    }
}
