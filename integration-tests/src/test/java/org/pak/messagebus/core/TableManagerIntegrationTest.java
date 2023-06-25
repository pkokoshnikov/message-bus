package org.pak.messagebus.core;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Slf4j
class TableManagerIntegrationTest extends BaseIntegrationTest {

    //TODO: check twice clean
    @Test
    void clearPartitionTest() throws InterruptedException {
        var messageProcessor = messageProcessorFactory.build().create();
        var messagePublisher = messagePublisherFactory.build().create();

        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now);
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now.plus(Duration.ofDays(1)));
        pgQueryService.createHistoryPartition(TEST_SUBSCRIPTION_NAME, now);
        pgQueryService.createHistoryPartition(TEST_SUBSCRIPTION_NAME, now.plus(Duration.ofDays(1)));
        messagePublisher.publish(new DefaultMessage<>(UUID.randomUUID().toString(), now, new TestMessage(TEST_VALUE)));
        messageProcessor.poolAndProcess();

        var cleaner = new TableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        cleaner.registerSubscription(TestMessage.MESSAGE_NAME, TEST_SUBSCRIPTION_NAME, 1);
        cleaner.registerMessage(TestMessage.MESSAGE_NAME, 1);
//        cleaner.cleanPartitions();
        cleaner.startCronJobs();
        cleaner.stopCronJobs();
        Thread.sleep(1000);
    }

    @Test
    void testStartCronJobSuccessfully() {
        var cleaner = new TableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        cleaner.registerSubscription(TestMessage.MESSAGE_NAME, TEST_SUBSCRIPTION_NAME, 1);
        cleaner.registerMessage(TestMessage.MESSAGE_NAME, 1);
        cleaner.startCronJobs();
        cleaner.stopCronJobs();
    }

    @Test
    void testStartCronJobFailed() {
        var cleaner = new TableManager(pgQueryService, "* * * * ?", "* * * * * ?");
        cleaner.registerSubscription(TestMessage.MESSAGE_NAME, TEST_SUBSCRIPTION_NAME, 1);
        cleaner.registerMessage(TestMessage.MESSAGE_NAME, 1);
        var exception = Assertions.assertThrows(RuntimeException.class, cleaner::startCronJobs);
        assertThat(exception.getMessage()).isEqualTo("CronExpression '* * * * ?' is invalid.");
    }
}
