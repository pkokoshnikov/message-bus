package org.pak.messagebus.core;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.pak.messagebus.pg.PgQueryService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.pak.messagebus.spring.SpringPersistenceService;
import org.pak.messagebus.spring.SpringTransactionService;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
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

import static java.util.Optional.ofNullable;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class BaseIntegrationTest {
    Logger log = org.slf4j.LoggerFactory.getLogger(this.getClass());
    static SubscriptionName TEST_SUBSCRIPTION_NAME = new SubscriptionName("test-subscription");
    static SchemaName TEST_SCHEMA = new SchemaName("public");
    static String TEST_VALUE = "test-value";
    static String TEST_EXCEPTION_MESSAGE = "test-exception-payload";
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
    JdbcTemplate jdbcTemplate;
    JsonbConverter jsonbConverter;
    SpringTransactionService springTransactionService;
    MessageProcessorFactory.MessageProcessorFactoryBuilder<TestMessage> messageProcessorFactory;
    MessagePublisherFactory.MessagePublisherFactoryBuilder<TestMessage> messagePublisherFactory;

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

        messagePublisherFactory = MessagePublisherFactory.<TestMessage>builder()
                .messageFactory(new StdMessageFactory())
                .messageName(TestMessage.MESSAGE_NAME)
                .queryService(pgQueryService)
                .traceIdExtractor(new NullTraceIdExtractor<>());

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
                .messageFactory(new StdMessageFactory())
                .messageListener(testMessage -> log.info("Handle testMessage: {}", testMessage))
                .queryService(pgQueryService)
                .transactionService(springTransactionService)
                .retryablePolicy(new StdRetryablePolicy())
                .blockingPolicy(new StdBlockingPolicy())
                .nonRetryablePolicy(new StdNonRetryablePolicy())
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
