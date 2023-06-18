package org.pak.messagebus.core;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.error.ExceptionClassifier;
import org.pak.messagebus.core.error.ExceptionType;
import org.pak.messagebus.pg.PgQueryService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.pak.messagebus.spring.SpringPersistenceService;
import org.pak.messagebus.spring.SpringTransactionService;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigInteger;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Optional.ofNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.Status.FAILED;
import static org.pak.messagebus.core.Status.PROCESSED;

@Testcontainers
@Slf4j
class MessageProcessorTest {
    static SubscriptionType<TestMessage> TEST_SUBSCRIPTION_TYPE =
            new SubscriptionType<>("test-subscription", TestMessage.MESSAGE_TYPE);
    static SchemaName TEST_SCHEMA = new SchemaName("public");
    static String TEST_VALUE = "test-value";
    static String TEST_EXCEPTION_MESSAGE = "test-exception-message";
    MessagePublisher<TestMessage> messagePublisher;
    PgQueryService pgQueryService;
    StringFormatter formatter = new StringFormatter();

    @Container
    PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.1"));
    Integer maxPollRecords = 1;
    JdbcTemplate jdbcTemplate;
    JsonbConverter jsonbConverter;
    SpringTransactionService springTransactionService;
    ExceptionClassifier testExceptionClassifier = exception -> {
        if (BlockingApplicationException.class.isAssignableFrom(exception.getClass())) {
            return ExceptionType.BLOCKING;
        } else if (NonRetryableApplicationException.class.isAssignableFrom(exception.getClass())) {
            return ExceptionType.NON_RETRYABLE;
        } else {
            return null;
        }
    };

    MessageProcessorFactory.MessageProcessorFactoryBuilder<TestMessage> messageProcessorFactory;

    @BeforeEach
    void setUp() {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setDatabaseName(postgres.getDatabaseName());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());

        jdbcTemplate = new JdbcTemplate(dataSource);
        jsonbConverter = new JsonbConverter();
        jsonbConverter.registerType(TestMessage.MESSAGE_TYPE.name(), TestMessage.MESSAGE_TYPE.messageClass());

        springTransactionService =
                new SpringTransactionService(new TransactionTemplate(new JdbcTransactionManager(dataSource) {}));
        pgQueryService = new PgQueryService(new SpringPersistenceService(jdbcTemplate), TEST_SCHEMA, jsonbConverter);

        messagePublisher =
                new MessagePublisher<>(TestMessage.MESSAGE_TYPE, TestMessage::getName, pgQueryService);

        pgQueryService.initMessageTable(TestMessage.MESSAGE_TYPE);
        pgQueryService.initSubscriptionTable(TestMessage.MESSAGE_TYPE, TEST_SUBSCRIPTION_TYPE);

        messageProcessorFactory = MessageProcessorFactory.<TestMessage>builder()
                .messageListener(testMessage -> log.info("Handle testMessage: {}", testMessage))
                .queryService(pgQueryService)
                .transactionService(springTransactionService)
                .blockingPolicy(new SimpleBlockingPolicy())
                .messageType(TestMessage.MESSAGE_TYPE)
                .subscriptionType(TEST_SUBSCRIPTION_TYPE)
                .maxPollRecords(maxPollRecords)
                .retryablePolicy(new SimpleRetryablePolicy())
                .exceptionClassifier(testExceptionClassifier)
                .traceIdExtractor(new NullTraceIdExtractor<>())
                .maxPollRecords(maxPollRecords);
    }

    @Test
    void testSubmitMessage() {
        TestMessage testMessage = new TestMessage(TEST_VALUE);

        messagePublisher.publish(testMessage);

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getMessage()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testSuccessHandle() {
        var messageProcessor = messageProcessorFactory.build().create();

        TestMessage testMessage1 = new TestMessage(TEST_VALUE);
        TestMessage testMessage2 = new TestMessage(TEST_VALUE + "-2");
        messagePublisher.publish(testMessage1);
        messagePublisher.publish(testMessage2);

        assertThat(messageProcessor.poolAndProcess()).isTrue();
        assertThat(messageProcessor.poolAndProcess()).isTrue();
        assertThat(messageProcessor.poolAndProcess()).isFalse();

        var testMessages = selectTestMessagesFromHistory();

        assertThat(testMessages).hasSize(2);
        assertThat(testMessages.get(0).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(0).getMessage()).isEqualTo(testMessage1);

        assertThat(testMessages.get(1).getStatus()).isEqualTo(PROCESSED);
        assertThat(testMessages.get(1).getMessage()).isEqualTo(testMessage2);
    }

    @Test
    void testHandleNonRetryableException() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new NonRetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);
        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory());

        assertThat(testMessageContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testHandleRetryableException() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> Duration.ofSeconds(600))
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        var testMessageBeforeHandleContainer = hasSize1AndGetFirst(selectTestMessages());

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
        assertThat(testMessageContainer.getExecuteAfter()).isAfterOrEqualTo(
                testMessageBeforeHandleContainer.getExecuteAfter().plus(Duration.ofSeconds(600)));

        messageProcessor.poolAndProcess();

        testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        //check that it wasn't retried before executeAfter
        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
        assertThat(testMessageContainer.getExecuteAfter()).isAfterOrEqualTo(
                testMessageBeforeHandleContainer.getExecuteAfter().plus(Duration.ofSeconds(60)));
    }

    @Test
    void testHandleRetryableException2() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> Duration.ofSeconds(0))
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        var testMessages = selectTestMessages();
        assertThat(testMessages).hasSize(1);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();

        messageProcessor.poolAndProcess();

        testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getAttempt()).isEqualTo(2);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();
    }

    @Test
    void testHandleRetryableExceptionFail() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new RetryableApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .retryablePolicy((e, attempt) -> {
                    if (attempt == 0) {
                        return Duration.ofSeconds(0);
                    } else {
                        return null;
                    }
                })
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        var testMessages = selectTestMessages();
        assertThat(testMessages).hasSize(1);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getAttempt()).isEqualTo(1);
        assertThat(testMessageContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageContainer.getStackTrace()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNotNull();

        messageProcessor.poolAndProcess();

        var testMessageHistoryContainer = hasSize1AndGetFirstHistory(selectTestMessagesFromHistory());

        assertThat(testMessageHistoryContainer.getStatus()).isEqualTo(FAILED);
        assertThat(testMessageHistoryContainer.getErrorMessage()).isEqualTo(TEST_EXCEPTION_MESSAGE);
        assertThat(testMessageHistoryContainer.getStackTrace()).isNotNull();
    }

    @Test
    void testBlockingExceptionIncorrectConfiguration() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new BlockingApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .exceptionClassifier(
                        exception -> BlockingApplicationException.class.isAssignableFrom(exception.getClass())
                                ? ExceptionType.BLOCKING : ExceptionType.RETRYABLE)
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        messageProcessor.poolAndProcess();
        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getMessage()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testDuplicateKeyPublish() {
        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var key = UUID.randomUUID().toString();

        messagePublisher.publish(key, testMessage);
        messagePublisher.publish(key, testMessage);

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getMessage()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testBlockingException() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new BlockingApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getMessage()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    MessageContainer<TestMessage> hasSize1AndGetFirst(List<MessageContainer<TestMessage>> testMessageContainers) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    MessageHistoryContainer<TestMessage> hasSize1AndGetFirstHistory(List<MessageHistoryContainer<TestMessage>> testMessageContainers) {
        assertThat(testMessageContainers).hasSize(1);
        return testMessageContainers.get(0);
    }

    List<MessageContainer<TestMessage>> selectTestMessages() {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.error_message, s.stack_trace, s.created_at, s.updated_at,
                            s.execute_after, e.payload
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${messageTable} e ON s.message_id = e.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", "test_subscription",
                        "messageTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> new MessageContainer<>(
                        rs.getObject("id", BigInteger.class),
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
                        rs.getString("stack_trace")));
    }

    List<MessageHistoryContainer<TestMessage>> selectTestMessagesFromHistory() {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.status, s.error_message, s.stack_trace, s.created_at, e.payload
                        FROM ${schema}.${subscriptionTableHistory} s JOIN ${messageTable} e ON s.message_id = e.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTableHistory", "test_subscription_history",
                        "messageTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> new MessageHistoryContainer<>(
                        rs.getObject("id", BigInteger.class),
                        rs.getInt("attempt"),
                        ofNullable(rs.getObject("created_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        Status.valueOf(rs.getString("status")),
                        jsonbConverter.toJsonb(rs.getObject("payload", PGobject.class)),
                        rs.getString("error_message"),
                        rs.getString("stack_trace")));
    }

    static class BlockingApplicationException extends RuntimeException {
        public BlockingApplicationException(String message) {
            super(message);
        }
    }

    static class RetryableApplicationException extends RuntimeException {
        public RetryableApplicationException(String message) {
            super(message);
        }
    }

    static class NonRetryableApplicationException extends RuntimeException {
        public NonRetryableApplicationException(String message) {
            super(message);
        }
    }
}
