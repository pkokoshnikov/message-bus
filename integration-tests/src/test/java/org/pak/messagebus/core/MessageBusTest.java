package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.pg.PgQueryService;
import org.pak.messagebus.pg.jsonb.JsonbConverter;
import org.pak.messagebus.spring.SpringPersistenceService;
import org.pak.messagebus.spring.SpringTransactionService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

@Testcontainers
@Slf4j
class MessageBusTest {
    static SubscriptionName SUBSCRIPTION_NAME_1 = new SubscriptionName("test-subscription-one");
    static SubscriptionName SUBSCRIPTION_NAME_2 = new SubscriptionName("test-subscription-two");
    static SchemaName SCHEMA_NAME = new SchemaName("public");
    JdbcTemplate jdbcTemplate;
    TransactionTemplate transactionTemplate;
    MessageBus messageBus;
    ViburDBCPDataSource dataSource;
    JsonbConverter jsonbConverter;
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.1"));

    @BeforeEach
    void setUp() {
        dataSource = new ViburDBCPDataSource();
        dataSource.setJdbcUrl(postgres.getJdbcUrl());
        dataSource.setPoolMaxSize(50);
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        dataSource.start();

        jdbcTemplate = new JdbcTemplate(dataSource);
        jsonbConverter = new JsonbConverter();
        jsonbConverter.registerType(MESSAGE_NAME.name(), TestMessage.class);
        transactionTemplate = new TransactionTemplate(new JdbcTransactionManager(dataSource) {});

        messageBus = new MessageBus(
                new PgQueryService(new SpringPersistenceService(jdbcTemplate), SCHEMA_NAME, jsonbConverter),
                new SpringTransactionService(transactionTemplate), new StdMessageFactory(),
                CronConfig.builder().build());
    }

    @AfterEach
    void tearDown() {
        dataSource.close();
    }

    @Test
    void publishSubscribeTest() throws InterruptedException {
        messageBus.registerPublisher(PublisherConfig.<TestMessage>builder()
                .messageName(MESSAGE_NAME)
                .clazz(TestMessage.class)
                .properties(PublisherConfig.Properties.builder()
                        .storageDays(10)
                        .build())
                .build());

        var countDownLatch = new CountDownLatch(2);
        var reference1 = new AtomicReference<TestMessage>();
        var reference2 = new AtomicReference<TestMessage>();

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageListener(message -> {
                    reference1.set(message.payload());
                    countDownLatch.countDown();
                })
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_1)
                .build());

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageListener(message -> {
                    reference2.set(message.payload());
                    countDownLatch.countDown();
                })
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_2)
                .build());

        messageBus.startSubscribers();
        TestMessage testMessage = new TestMessage("test-name");
        messageBus.publish(testMessage);

        countDownLatch.await();
        messageBus.stopSubscribers();

        var handledMessage1 = reference1.get();
        assertThat(handledMessage1).isEqualTo(testMessage);
        var handledMessage2 = reference2.get();
        assertThat(handledMessage2).isEqualTo(testMessage);
    }

    //only for manual running
    @Test
    @Disabled
    void performanceTest() throws InterruptedException {
        messageBus.registerPublisher(PublisherConfig.<TestMessage>builder()
                .messageName(MESSAGE_NAME)
                .build());

        var countDownLatch = new CountDownLatch(100_000);

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageListener(message -> countDownLatch.countDown())
                .messageName(MESSAGE_NAME)
                .subscriptionName(SUBSCRIPTION_NAME_1)
                .properties(SubscriberConfig.Properties.builder()
                        .concurrency(50)
                        .build())
                .build());

        messageBus.startSubscribers();

        for (int i = 0; i < 100_000; i++) {
            TestMessage testMessage = new TestMessage("test-name");
            messageBus.publish(testMessage);
        }

        countDownLatch.await();
        messageBus.stopSubscribers();
    }
}
