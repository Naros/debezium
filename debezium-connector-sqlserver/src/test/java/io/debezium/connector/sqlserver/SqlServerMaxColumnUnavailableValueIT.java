/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Integration tests verifying that VARBINARY(MAX), VARCHAR(MAX), and NVARCHAR(MAX) columns
 * use the {@code unavailable.value.placeholder} when not changed in an UPDATE operation.
 *
 * @author Chris Cranford
 */
public class SqlServerMaxColumnUnavailableValueIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.setAutoCommit(false);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() throws SQLException {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Verifies that when an UPDATE does not change a VARCHAR(MAX), NVARCHAR(MAX), or VARBINARY(MAX)
     * column, the resulting CDC event contains the unavailable value placeholder rather than NULL.
     * The update mask ({{__$update_mask}}) is used to distinguish unchanged MAX columns (NULL in
     * CDC table, placeholder in event) from columns explicitly set to NULL (NULL in event).
     */
    @Test
    @FixFor("DBZ-6884")
    void unchangedMaxColumnsUseUnavailablePlaceholder() throws Exception {
        // Insert the initial row before enabling CDC to avoid duplicate events
        connection.execute(
                "CREATE TABLE dbz6884 (" +
                        "  id INT PRIMARY KEY, " +
                        "  col_varchar_max VARCHAR(MAX), " +
                        "  col_nvarchar_max NVARCHAR(MAX), " +
                        "  col_varbinary_max VARBINARY(MAX), " +
                        "  col_int INT" +
                        ")",
                "INSERT INTO dbz6884 VALUES (1, 'text', N'ntext', 0x0102, 100)");
        TestHelper.enableTableCdc(connection, "dbz6884");
        connection.commit();

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.dbz6884")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Consume the snapshot record
        final SourceRecord snapshot = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.dbz6884").get(0);
        assertThat(snapshot).isNotNull();

        // Wait for streaming to start before executing DML
        TestHelper.waitForStreamingStarted();

        // Update only col_int — MAX columns are unchanged
        connection.execute("UPDATE dbz6884 SET col_int = 200 WHERE id = 1");
        connection.commit();

        // Consume the update event
        final SourceRecord updateRecord = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.dbz6884").get(0);
        assertThat(updateRecord).isNotNull();

        final Struct after = ((Struct) updateRecord.value()).getStruct("after");
        assertThat(after).isNotNull();

        final String placeholder = RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER;
        final ByteBuffer binaryPlaceholder = ByteBuffer.wrap(placeholder.getBytes());

        // Unchanged MAX columns should carry the unavailable value placeholder
        assertThat(after.get("col_varchar_max")).isEqualTo(placeholder);
        assertThat(after.get("col_nvarchar_max")).isEqualTo(placeholder);
        assertThat(after.get("col_varbinary_max")).isEqualTo(binaryPlaceholder);

        // The changed column should have its new value
        assertThat(after.get("col_int")).isEqualTo(200);

        stopConnector();
    }

    /**
     * Verifies that when a VARCHAR(MAX), NVARCHAR(MAX), or VARBINARY(MAX) column is explicitly
     * set to NULL in an UPDATE, the resulting CDC event contains NULL (not the placeholder).
     * The update mask bit for such columns is set, distinguishing them from unchanged columns.
     */
    @Test
    @FixFor("DBZ-6884")
    void explicitlyNulledMaxColumnsRemainNull() throws Exception {
        // Insert the initial row before enabling CDC to avoid duplicate events
        connection.execute(
                "CREATE TABLE dbz6884b (" +
                        "  id INT PRIMARY KEY, " +
                        "  col_varchar_max VARCHAR(MAX), " +
                        "  col_nvarchar_max NVARCHAR(MAX), " +
                        "  col_varbinary_max VARBINARY(MAX), " +
                        "  col_int INT" +
                        ")",
                "INSERT INTO dbz6884b VALUES (1, 'text', N'ntext', 0x0102, 100)");
        TestHelper.enableTableCdc(connection, "dbz6884b");
        connection.commit();

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.dbz6884b")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        // Consume the snapshot record
        final SourceRecord snapshot = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.dbz6884b").get(0);
        assertThat(snapshot).isNotNull();

        // Wait for streaming to start before executing DML
        TestHelper.waitForStreamingStarted();

        // Explicitly set MAX columns to NULL
        connection.execute("UPDATE dbz6884b SET col_varchar_max = NULL, col_nvarchar_max = NULL, col_varbinary_max = NULL WHERE id = 1");
        connection.commit();

        final SourceRecord updateRecord = consumeRecordsByTopic(1).recordsForTopic("server1.testDB1.dbo.dbz6884b").get(0);
        assertThat(updateRecord).isNotNull();

        final Struct after = ((Struct) updateRecord.value()).getStruct("after");
        assertThat(after).isNotNull();

        // Explicitly nulled MAX columns should remain NULL, not receive the placeholder
        assertThat(after.schema().field("col_varchar_max").schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(after.get("col_varchar_max")).isNull();
        assertThat(after.schema().field("col_nvarchar_max").schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(after.get("col_nvarchar_max")).isNull();
        assertThat(after.get("col_varbinary_max")).isNull();

        stopConnector();
    }
}
