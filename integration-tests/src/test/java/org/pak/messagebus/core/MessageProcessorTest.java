package org.pak.messagebus.core;


import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.pak.messagebus.core.error.RetrayablePersistenceException;
import org.pak.messagebus.pg.PgQueryService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.pak.messagebus.spring.SpringPersistenceService;
import org.pak.messagebus.spring.SpringTransactionService;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.Status.FAILED;
import static org.pak.messagebus.core.Status.PROCESSED;

@Testcontainers
@Slf4j
class MessageProcessorTest {
    static SubscriptionName TEST_SUBSCRIPTION_NAME = new SubscriptionName("test-subscription");
    static SchemaName TEST_SCHEMA = new SchemaName("public");
    static String TEST_VALUE = "test-value";
    static String TEST_EXCEPTION_MESSAGE = "test-exception-payload";
    MessagePublisher<TestMessage> messagePublisher;
    QueueMessagePublisher<TestMessage> queueMessagePublisher;
    PgQueryService pgQueryService;
    StringFormatter formatter = new StringFormatter();

    static Network network = Network.newNetwork();

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.1"))
            .withNetwork(network)
            .withNetworkAliases("postgres");
    @Container
    static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);
    static Proxy postgresqlProxy;
    Integer maxPollRecords = 1;
    JdbcTemplate jdbcTemplate;
    JsonbConverter jsonbConverter;
    SpringTransactionService springTransactionService;
    MessageProcessorFactory.MessageProcessorFactoryBuilder<TestMessage> messageProcessorFactory;

    @BeforeAll
    static void beforeAll() throws IOException {
        var toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresqlProxy = toxiproxyClient.createProxy("postgresql", "0.0.0.0:8666", "postgres:5432");
    }

    @BeforeEach
    void setUp() {
        var jdbcUrl = "jdbc:postgresql://%s:%d/%s".formatted(toxiproxy.getHost(), toxiproxy.getMappedPort(8666),
                postgres.getDatabaseName());

        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setDatabaseName(postgres.getDatabaseName());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());

        jdbcTemplate = new JdbcTemplate(dataSource);
        jsonbConverter = new JsonbConverter();
        jsonbConverter.registerType(TestMessage.MESSAGE_NAME.name(), TestMessage.class);

        springTransactionService = new SpringTransactionService(new TransactionTemplate(
                new JdbcTransactionManager(dataSource) {}));

        pgQueryService = new PgQueryService(new SpringPersistenceService(jdbcTemplate), TEST_SCHEMA, jsonbConverter);

        messagePublisher = new MessagePublisher<>(TestMessage.MESSAGE_NAME, TestMessage::getName, pgQueryService,
                new DefaultMessageFactory());

        queueMessagePublisher = new QueueMessagePublisher<>(TestMessage.MESSAGE_NAME, TestMessage::getName,
                pgQueryService, springTransactionService);

        pgQueryService.initMessageTable(TestMessage.MESSAGE_NAME);
        pgQueryService.initSubscriptionTable(TestMessage.MESSAGE_NAME, TEST_SUBSCRIPTION_NAME);

        var now = Instant.now().truncatedTo(ChronoUnit.DAYS);
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now);
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now.plus(Duration.ofDays(1)));
        pgQueryService.createHistoryPartition(TEST_SUBSCRIPTION_NAME, now);
        pgQueryService.createHistoryPartition(TEST_SUBSCRIPTION_NAME, now.plus(Duration.ofDays(1)));

        messageProcessorFactory = MessageProcessorFactory.<TestMessage>builder()
                .messageFactory(new DefaultMessageFactory())
                .messageListener(testMessage -> log.info("Handle testMessage: {}", testMessage))
                .queryService(pgQueryService)
                .transactionService(springTransactionService)
                .retryablePolicy(new SimpleRetryablePolicy())
                .blockingPolicy(new SimpleBlockingPolicy())
                .nonRetryablePolicy(new SimpleNonRetryablePolicy())
                .messageName(TestMessage.MESSAGE_NAME)
                .subscriptionName(TEST_SUBSCRIPTION_NAME)
                .traceIdExtractor(object -> null)
                .properties(SubscriberConfig.Properties.builder().build());
    }

    @AfterEach
    void clear() {
        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", "test_subscription")));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${subscriptionTable}_history",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", "test_subscription")));

        jdbcTemplate.update(formatter.execute("DROP TABLE IF EXISTS ${schema}.${messageTable}",
                Map.of("schema", TEST_SCHEMA.value(),
                        "messageTable", "test_message")));
    }

    @Test
    void testSubmitMessage() {
        TestMessage testMessage = new TestMessage(TEST_VALUE);

        messagePublisher.publish(testMessage);

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getPayload()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getOriginatedTime()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testSubmitBatchMessages() {
        var testMessage1 = new DefaultMessage<>(UUID.randomUUID().toString(), Instant.now(),
                new TestMessage(TEST_VALUE + "_1"));
        var testMessage2 = new DefaultMessage<>(UUID.randomUUID().toString(), Instant.now(),
                new TestMessage(TEST_VALUE + "_2"));
        var testMessage3 = new DefaultMessage<>(UUID.randomUUID().toString(), Instant.now(),
                new TestMessage(TEST_VALUE + "_3"));

        List<Message<TestMessage>> messages = List.of(testMessage1, testMessage2, testMessage3);
        queueMessagePublisher.publish(messages);
        queueMessagePublisher.publish(messages);//check duplicates

        var testMessagesContainers = selectTestMessages();

        assertThat(testMessagesContainers).hasSize(3);

        messages.forEach(message -> {
            var testMessageContainer = testMessagesContainers.stream()
                    .filter(tmc -> tmc.getKey().equals(message.key()))
                    .findFirst()
                    .get();

            assertThat(testMessageContainer.getPayload()).isEqualTo(message.payload());
            assertThat(testMessageContainer.getCreated()).isNotNull();
            assertThat(testMessageContainer.getOriginatedTime()).isEqualTo(message.originatedTime());
            assertThat(testMessageContainer.getUpdated()).isNull();
            assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
            assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
            assertThat(testMessageContainer.getErrorMessage()).isNull();
            assertThat(testMessageContainer.getStackTrace()).isNull();
        });
    }

    @Test
    void testSuccessHandle() throws RetrayablePersistenceException {
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
                .nonRetryablePolicy(
                        exception -> NonRetryableApplicationException.class.isAssignableFrom(exception.getClass()))
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
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(600)))
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
                .retryablePolicy((e, attempt) -> of(Duration.ofSeconds(0)))
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
                        return Optional.of(Duration.ofSeconds(0));
                    } else {
                        return Optional.empty();
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
    void testDuplicateKeyPublish() {
        TestMessage testMessage = new TestMessage(TEST_VALUE);
        var key = UUID.randomUUID().toString();
        var originatedTime = Instant.now();
        messagePublisher.publish(new DefaultMessage<>(key, originatedTime, testMessage));
        messagePublisher.publish(new DefaultMessage<>(key, originatedTime, testMessage));

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getPayload()).isEqualTo(testMessage);
        assertThat(testMessageContainer.getCreated()).isNotNull();
        assertThat(testMessageContainer.getUpdated()).isNull();
        assertThat(testMessageContainer.getExecuteAfter()).isNotNull();
        assertThat(testMessageContainer.getAttempt()).isEqualTo(0);
        assertThat(testMessageContainer.getErrorMessage()).isNull();
        assertThat(testMessageContainer.getStackTrace()).isNull();
    }

    @Test
    void testTimeoutException() throws IOException {
        var messageProcessor = messageProcessorFactory.build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        var timeout = postgresqlProxy.toxics().timeout("pg-timeout", ToxicDirection.DOWNSTREAM, 1000);

        Assertions.assertThrows(RetrayablePersistenceException.class, messageProcessor::poolAndProcess);

        timeout.remove();
    }

    @Test
    void testBlockingException() {
        var messageProcessor = messageProcessorFactory.messageListener(testMessage -> {
                    throw new BlockingApplicationException(TEST_EXCEPTION_MESSAGE);
                })
                .blockingPolicy(new BlockingPolicy() {
                    @Override
                    public boolean isBlocked(Exception exception) {
                        return BlockingApplicationException.class.isAssignableFrom(exception.getClass());
                    }

                    @Override
                    public @NonNull Duration apply(Exception exception) {
                        return Duration.ofMillis(30_000);
                    }
                })
                .build().create();

        TestMessage testMessage = new TestMessage(TEST_VALUE);
        messagePublisher.publish(testMessage);

        messageProcessor.poolAndProcess();

        var testMessageContainer = hasSize1AndGetFirst(selectTestMessages());

        assertThat(testMessageContainer.getPayload()).isEqualTo(testMessage);
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
                            s.execute_after, m.payload, m.originated_at, m.key
                        FROM ${schema}.${subscriptionTable} s JOIN ${schema}.${messageTable} m ON s.message_id = m.id""",
                Map.of("schema", TEST_SCHEMA.value(),
                        "subscriptionTable", "test_subscription",
                        "messageTable", "test_message"));

        return jdbcTemplate.query(query,
                (rs, rowNum) -> new MessageContainer<>(
                        rs.getObject("id", BigInteger.class),
                        rs.getObject("message_id", BigInteger.class),
                        rs.getString("key"),
                        rs.getInt("attempt"),
                        ofNullable(rs.getObject("execute_after", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("created_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("updated_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        ofNullable(rs.getObject("originated_at", OffsetDateTime.class))
                                .map(OffsetDateTime::toInstant).orElse(null),
                        jsonbConverter.toJsonb(rs.getObject("payload", PGobject.class)),
                        rs.getString("error_message"),
                        rs.getString("stack_trace")));
    }

    List<MessageHistoryContainer<TestMessage>> selectTestMessagesFromHistory() {
        var query = formatter.execute("""
                        SELECT s.id, s.message_id, s.attempt, s.status, s.error_message, s.stack_trace, s.created_at, m.payload
                        FROM ${schema}.${subscriptionTableHistory} s JOIN ${schema}.${messageTable} m ON s.message_id = m.id""",
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
