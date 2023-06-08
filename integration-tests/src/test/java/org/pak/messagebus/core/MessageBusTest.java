package org.pak.messagebus.core;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.core.error.ExceptionClassifier;
import org.pak.messagebus.core.error.ExceptionType;
import org.pak.messagebus.pg.PgQueryServiceFactory;
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

import javax.annotation.Nonnull;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.MESSAGE_TYPE;

@Testcontainers
@Slf4j
class MessageBusTest {
    static SubscriptionType<TestMessage> SUBSCRIPTION_TYPE_1 =
            new SubscriptionType<>("test-subscription-one", MESSAGE_TYPE);
    static SubscriptionType<TestMessage> SUBSCRIPTION_TYPE_2 =
            new SubscriptionType<>("test-subscription-two", MESSAGE_TYPE);
    static SchemaName SCHEMA_NAME = new SchemaName("public");
    JdbcTemplate jdbcTemplate;
    TransactionTemplate transactionTemplate;
    MessageBus messageBus;
    ViburDBCPDataSource dataSource;
    @Container
    PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15.1"));

    @BeforeEach
    void setUp() {
        dataSource = new ViburDBCPDataSource();
        dataSource.setJdbcUrl(postgres.getJdbcUrl());
        dataSource.setPoolMaxSize(50);
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        dataSource.start();

        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new JdbcTransactionManager(dataSource) {});
        messageBus = new MessageBus(
                new PgQueryServiceFactory(new SpringPersistenceService(jdbcTemplate), SCHEMA_NAME),
                new SpringTransactionService(transactionTemplate), new ExceptionClassifier() {
            @Nonnull
            @Override
            public ExceptionType classify(Exception exception) {
                return ExceptionType.RETRYABLE;
            }
        });
    }

    @AfterEach
    void tearDown() {
        dataSource.close();
    }

    @Test
    void publishSubscribeTest() throws InterruptedException {
        messageBus.registerPublisher(PublisherConfig.<TestMessage>builder()
                .messageType(MESSAGE_TYPE)
                .build());

        var countDownLatch = new CountDownLatch(2);
        var reference1 = new AtomicReference<TestMessage>();
        var reference2 = new AtomicReference<TestMessage>();

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageType(MESSAGE_TYPE)
                .subscriptionType(SUBSCRIPTION_TYPE_1)
                .messageListener(message -> {
                    reference1.set(message);
                    countDownLatch.countDown();
                })
                .build());

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageType(MESSAGE_TYPE)
                .subscriptionType(SUBSCRIPTION_TYPE_2)
                .messageListener(message -> {
                    reference2.set(message);
                    countDownLatch.countDown();
                })
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

    @Test
    @Disabled
        //only for manual running
    void performanceTest() throws InterruptedException {
        messageBus.registerPublisher(PublisherConfig.<TestMessage>builder()
                .messageType(MESSAGE_TYPE)
                .build());

        var countDownLatch = new CountDownLatch(100_000);

        messageBus.registerSubscriber(SubscriberConfig.<TestMessage>builder()
                .messageType(MESSAGE_TYPE)
                .subscriptionType(SUBSCRIPTION_TYPE_1)
                .messageListener(message -> countDownLatch.countDown())
                .concurrency(50)
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
