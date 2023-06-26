package org.pak.messagebus.core;


import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pak.messagebus.pg.PgQueryService;
import org.pak.messagebus.spring.SpringPersistenceService;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vibur.dbcp.ViburDBCPDataSource;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pak.messagebus.core.TestMessage.MESSAGE_NAME;

@Testcontainers
@Slf4j
class TableManagerIntegrationTest extends BaseIntegrationTest {

    @BeforeEach
    void setUp() {
        dataSource = setupDatasource();
        springTransactionService = setupSpringTransactionService(dataSource);
        jdbcTemplate = setupJdbcTemplate(dataSource);
        persistenceService = setupPersistenceService(jdbcTemplate);
        jsonbConverter = setupJsonbConverter();
        pgQueryService = setupQueryService(persistenceService, jsonbConverter);
        tableManager = setupTableManager(pgQueryService);

        tableManager.registerMessage(MESSAGE_NAME, 30);
        tableManager.registerSubscription(MESSAGE_NAME, SUBSCRIPTION_NAME_1, 30);
    }

    @AfterEach
    void tearDown() {
        clearTables(jdbcTemplate);
        tableManager.stopCronJobs();
    }

    @Test
    void cleanMessagePartitionTest() {
        String table = TestMessage.MESSAGE_NAME.name().replace("-", "_");
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now);
        pgQueryService.createMessagePartition(TestMessage.MESSAGE_NAME, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(table);
        assertThat(partitions).hasSize(4);
        assertPartitions(table, partitions);

        var tm = new TableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        tm.registerMessage(TestMessage.MESSAGE_NAME, 2);
        tm.cleanPartitions();

        partitions = selectPartitions(table);
        assertThat(partitions).hasSize(2);
        assertPartitions(table, partitions);

        tm.cleanPartitions();

        partitions = selectPartitions(table);
        assertThat(partitions).hasSize(2);
        assertPartitions(table, partitions);
    }

    @Test
    void cleanSubscriptionPartitionTest() {
        var historyTable = SUBSCRIPTION_NAME_1.name().replace("-", "_") + "_history";
        var now = Instant.now().truncatedTo(ChronoUnit.DAYS).minus(Duration.ofDays(10));
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, now);
        pgQueryService.createHistoryPartition(SUBSCRIPTION_NAME_1, now.plus(Duration.ofDays(1)));

        List<String> partitions = selectPartitions(historyTable);
        assertThat(partitions).hasSize(4);
        assertPartitions(historyTable, partitions);

        var tm = new TableManager(pgQueryService, "* * * * * ?", "* * * * * ?");
        tm.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 2);
        tm.cleanPartitions();

        partitions = selectPartitions(historyTable);
        assertThat(partitions).hasSize(2);
        assertPartitions(historyTable, partitions);

        tm.cleanPartitions();

        partitions = selectPartitions(historyTable);
        assertThat(partitions).hasSize(2);
        assertPartitions(historyTable, partitions);
    }

    @Test
    void testCreateMessagePartitions() {
        String table = TestMessage.MESSAGE_NAME.name().replace("-", "_");
        tableManager.registerMessage(TestMessage.MESSAGE_NAME, 1);

        List<String> partitions = selectPartitions(table);

        assertThat(partitions).hasSize(2);
        assertPartitions(table, partitions);
    }

    @Test
    void testCreateSubscriptionPartitions() {
        var historyTable = SUBSCRIPTION_NAME_1.name().replace("-", "_") + "_history";
        tableManager.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);

        List<String> partitions = selectPartitions(historyTable);

        assertThat(partitions).hasSize(2);

        assertPartitions(historyTable, partitions);
    }

    @Test
    void testStartCronJobSuccessfully() {
        tableManager.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        tableManager.registerMessage(TestMessage.MESSAGE_NAME, 1);
        tableManager.startCronJobs();
        tableManager.stopCronJobs();
    }

    @Test
    void testStartCronJobFailed() {
        var corruptedTableManager = new TableManager(pgQueryService, "* * * * ?", "* * * * * ?");
        corruptedTableManager.registerSubscription(TestMessage.MESSAGE_NAME, SUBSCRIPTION_NAME_1, 1);
        corruptedTableManager.registerMessage(TestMessage.MESSAGE_NAME, 1);
        var exception = Assertions.assertThrows(RuntimeException.class, corruptedTableManager::startCronJobs);
        assertThat(exception.getMessage()).isEqualTo("CronExpression '* * * * ?' is invalid.");
    }

    private void assertPartitions(String tableName, List<String> partitions) {
        var params = Map.of("table", tableName);
        partitions.forEach(partition -> {
            var matches = Pattern.compile(formatter.execute("${table}_\\d{4}_\\d{2}_\\d{2}", params))
                    .matcher(partition)
                    .matches();
            assertThat(matches).isTrue();
        });
    }

    @NotNull
    private List<String> selectPartitions(String tableName) {
        Map<String, String> formatParams = Map.of("schema", TEST_SCHEMA.value(),
                "table", tableName);
        var query = formatter.execute("""
                        SELECT inhrelid::regclass AS partition
                        FROM   pg_catalog.pg_inherits
                        WHERE  inhparent = '${schema}.${table}'::regclass;""",
                formatParams);
        return jdbcTemplate.query(query, (rs, rowNum) -> rs.getString("partition"));
    }
}
