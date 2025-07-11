/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection.DatabaseLocales;
import io.debezium.connector.binlog.metrics.BinlogSnapshotChangeEventSourceMetrics;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * An abstract implementation of {@link SnapshotChangeEventSource} for binlog-based connectors.
 *
 * @author Chris Cranford
 */
public abstract class BinlogSnapshotChangeEventSource<P extends BinlogPartition, O extends BinlogOffsetContext<?>>
        extends RelationalSnapshotChangeEventSource<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogSnapshotChangeEventSource.class);
    private static final Logger ROW_ESTIMATE_LOGGER = LoggerFactory.getLogger(BinlogSnapshotChangeEventSource.class.getName() + ".RowEstimate");

    private final BinlogConnectorConfig connectorConfig;
    private final BinlogConnectorConnection connection;
    private final RelationalTableFilters filters;
    private final BinlogSnapshotChangeEventSourceMetrics<P> metrics;
    private final BinlogDatabaseSchema<P, O, ?, ?> databaseSchema;
    private final Set<SchemaChangeEvent> schemaEvents = new LinkedHashSet<>();
    private final BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor;
    private final Runnable preSnapshotAction;
    private Set<TableId> delayedSchemaSnapshotTables = Collections.emptySet();
    private long globalLockAcquiredAt = -1;
    private long tableLockAcquiredAt = -1;

    public BinlogSnapshotChangeEventSource(BinlogConnectorConfig connectorConfig,
                                           MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                           BinlogDatabaseSchema<P, O, ?, ?> schema,
                                           EventDispatcher<P, TableId> dispatcher,
                                           Clock clock,
                                           BinlogSnapshotChangeEventSourceMetrics<P> metrics,
                                           BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
                                           Runnable preSnapshotAction,
                                           NotificationService<P, O> notificationService,
                                           SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.connection = connectionFactory.mainConnection();
        this.filters = connectorConfig.getTableFilters();
        this.metrics = metrics;
        this.databaseSchema = schema;
        this.lastEventProcessor = lastEventProcessor;
        this.preSnapshotAction = preSnapshotAction;
    }

    @Override
    protected SnapshotContext<P, O> prepare(P partition, boolean onDemand) {
        return new BinlogSnapshotContext<>(partition, onDemand);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<P, O> ctx) throws Exception {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOGGER.info("Read list of available databases");
        final List<String> databaseNames = connection.availableDatabases();
        LOGGER.info("\t list of available databases is: {}", databaseNames);

        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
        // build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
        // we are reading the database names from the database and not taking them from the user ...
        LOGGER.info("Read list of available tables in each database");
        final Set<TableId> tableIds = new HashSet<>();
        final Set<String> readableDatabaseNames = new HashSet<>();
        for (String dbName : databaseNames) {
            try {
                // MySQL sometimes considers some local files as databases (see DBZ-164),
                // so we will simply try each one and ignore the problematic ones ...
                connection.query("SHOW FULL TABLES IN " + connection.quoteIdentifier(dbName) + " where Table_Type = 'BASE TABLE'", rs -> {
                    while (rs.next()) {
                        TableId id = new TableId(dbName, null, rs.getString(1));
                        tableIds.add(id);
                    }
                });
                readableDatabaseNames.add(dbName);
            }
            catch (SQLException e) {
                // We were unable to execute the query or process the results, so skip this ...
                LOGGER.warn("\t skipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
            }
        }
        final Set<String> includedDatabaseNames = readableDatabaseNames.stream().filter(filters.databaseFilter()).collect(Collectors.toSet());
        LOGGER.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);
        return tableIds;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<P, O> snapshotContext)
            throws SQLException {
        // Set the transaction isolation level to REPEATABLE READ. This is the default, but the default can be changed
        // which is why we explicitly set it here.
        //
        // With REPEATABLE READ, all SELECT queries within the scope of a transaction (which we don't yet have) will read
        // from the same MVCC snapshot. Thus each plain (non-locking) SELECT statements within the same transaction are
        // consistent also with respect to each other.
        //
        // See: https://dev.mysql.com/doc/refman/8.2/en/set-transaction.html
        // See: https://dev.mysql.com/doc/refman/8.2/en/innodb-transaction-isolation-levels.html
        // See: https://dev.mysql.com/doc/refman/8.2/en/innodb-consistent-read.html
        connection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.executeWithoutCommitting("SET SESSION lock_wait_timeout=" + connectorConfig.snapshotLockTimeout().getSeconds());
        try {
            connection.executeWithoutCommitting("SET SESSION innodb_lock_wait_timeout=" + connectorConfig.snapshotLockTimeout().getSeconds());
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to set innodb_lock_wait_timeout", e);
        }

        // ------------------------------------
        // LOCK TABLES
        // ------------------------------------
        // Obtain read lock on all tables. This statement closes all open tables and locks all tables
        // for all databases with a global read lock, and it prevents ALL updates while we have this lock.
        // It also ensures that everything we do while we have this lock will be consistent.
        if (connectorConfig.getSnapshotLockingStrategy().isLockingEnabled() && connectorConfig.isGlobalLockUseRequested()) {
            try {
                globalLock();
                metrics.setGlobalLockAcquired();
            }
            catch (SQLException e) {
                LOGGER.info("Unable to flush and acquire global read lock, will use table read locks after reading table names");
                // Continue anyway, since RDS (among others) don't allow setting a global lock
                assert !isGloballyLocked();
            }
            if (connectorConfig.getSnapshotLockingStrategy().isIsolationLevelResetOnFlush()) {
                // FLUSH TABLES resets TX and isolation level
                connection.executeWithoutCommitting("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext) throws SQLException {
        if (connectorConfig.getSnapshotLockingStrategy().isMinimalLockingEnabled()) {
            if (isGloballyLocked()) {
                globalUnlock();
            }
            if (isTablesLocked()) {
                // We could not acquire a global read lock and instead had to obtain individual table-level read locks
                // using 'FLUSH TABLE <tableName> WITH READ LOCK'. However, if we were to do this, the 'UNLOCK TABLES'
                // would implicitly commit our active transaction, and this would break our consistent snapshot logic.
                // Therefore, we cannot unlock the tables here!
                // https://dev.mysql.com/doc/refman/8.2/en/flush.html
                LOGGER.warn("Tables were locked explicitly, but to get a consistent snapshot we cannot release the locks until we've read all tables.");
            }
        }
    }

    @Override
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext) throws Exception {
        if (isGloballyLocked()) {
            globalUnlock();
        }
        if (isTablesLocked()) {
            tableUnlock();
            if (!delayedSchemaSnapshotTables.isEmpty()) {
                schemaEvents.clear();
                if (connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
                    createSchemaEventsForTables(snapshotContext, delayedSchemaSnapshotTables, false);
                }
                else {
                    int snapshotMaxThreads = connectionPool.size();
                    LOGGER.info("Creating delayed schema snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
                    ExecutorService executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
                    try {
                        createSchemaEventsForTables(snapshotContext, delayedSchemaSnapshotTables, false, executorService);
                    }
                    finally {
                        executorService.shutdownNow();
                    }
                }

                for (final SchemaChangeEvent event : schemaEvents) {
                    if (databaseSchema.storeOnlyCapturedTables()
                            && event.getDatabase() != null
                            && !event.getDatabase().isEmpty()
                            && !connectorConfig.getTableFilters().databaseFilter().test(event.getDatabase())) {
                        LOGGER.debug("Skipping schema event as it belongs to a non-captured database: '{}'", event);
                        continue;
                    }

                    LOGGER.debug("Processing schema event {}", event);

                    final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
                    snapshotContext.offset.event(tableId, getClock().currentTime());
                    dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, snapshotContext.offset, tableId, (receiver) -> receiver.schemaChangeEvent(event));
                }

                // Make schema available for snapshot source
                databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
            }
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<P, O> ctx, O previousOffset) throws Exception {
        if (!isGloballyLocked() && !isTablesLocked() && connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
            return;
        }

        if (previousOffset != null) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        final O offsetContext = getInitialOffsetContext(connectorConfig);
        ctx.offset = offsetContext;

        setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(offsetContext, connection, snapshotterService);
        tryStartingSnapshot(ctx);
    }

    protected abstract O getInitialOffsetContext(BinlogConnectorConfig connectorConfig);

    protected abstract void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(O offsetContext,
                                                                                    BinlogConnectorConnection connection,
                                                                                    SnapshotterService snapshotterService)
            throws Exception;

    private void addSchemaEvent(RelationalSnapshotContext<P, O> snapshotContext, String database, String ddl) {
        List<SchemaChangeEvent> schemaChangeEvents = databaseSchema.parseSnapshotDdl(snapshotContext.partition, ddl, database,
                snapshotContext.offset, clock.currentTimeAsInstant());
        schemaEvents.addAll(new LinkedHashSet<>(schemaChangeEvents));
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<P, O> snapshotContext,
                                      O offsetContext,
                                      SnapshottingTask snapshottingTask)
            throws Exception {
        Set<TableId> capturedSchemaTables;
        if (twoPhaseSchemaSnapshot()) {
            // Capture schema of captured tables after they are locked
            tableLock(snapshotContext);
            determineSnapshotOffset(snapshotContext, offsetContext);
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Table level locking is in place, the schema will be capture in two phases, now capturing: {}", capturedSchemaTables);
            delayedSchemaSnapshotTables = Collect.minus(snapshotContext.capturedSchemaTables, snapshotContext.capturedTables);
            LOGGER.info("Tables for delayed schema capture: {}", delayedSchemaSnapshotTables);
        }
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        final Map<String, List<TableId>> tablesToRead = capturedSchemaTables.stream()
                .collect(Collectors.groupingBy(TableId::catalog, LinkedHashMap::new, Collectors.toList()));
        final Set<String> databases = tablesToRead.keySet();

        if (!snapshottingTask.isOnDemand()) {
            // Record default charset
            addSchemaEvent(snapshotContext, "", connection.setStatementFor(connection.readCharsetSystemVariables()));
        }

        for (TableId tableId : capturedSchemaTables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while emitting initial DROP TABLE events");
            }
            addSchemaEvent(snapshotContext, tableId.catalog(), "DROP TABLE IF EXISTS " + connection.quotedTableIdString(tableId));
        }

        final Map<String, DatabaseLocales> databaseCharsets = connection.readDatabaseCollations();

        ExecutorService executorService = null;
        if (!connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
            int snapshotMaxThreads = connectionPool.size();
            LOGGER.info("Creating schema snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
            executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
        }
        try {
            for (String database : databases) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while reading structure of schema " + databases);
                }

                if (!snapshottingTask.isOnDemand()) {
                    // in case of blocking snapshot we want to read structures only for collections specified in the signal
                    LOGGER.info("Reading structure of database '{}'", database);
                    addSchemaEvent(snapshotContext, database, "DROP DATABASE IF EXISTS " + connection.quoteIdentifier(database));
                    final StringBuilder createDatabaseDdl = new StringBuilder("CREATE DATABASE " + connection.quoteIdentifier(database));
                    final DatabaseLocales defaultDatabaseLocales = databaseCharsets.get(database);
                    if (defaultDatabaseLocales != null) {
                        defaultDatabaseLocales.appendToDdlStatement(database, createDatabaseDdl);
                    }
                    addSchemaEvent(snapshotContext, database, createDatabaseDdl.toString());
                    addSchemaEvent(snapshotContext, database, "USE " + connection.quoteIdentifier(database));
                }

                if (connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
                    createSchemaEventsForTables(snapshotContext, tablesToRead.get(database), true);
                }
                else {
                    assert executorService != null;
                    createSchemaEventsForTables(snapshotContext, tablesToRead.get(database), true, executorService);
                }
            }
        }
        finally {
            if (executorService != null) {
                executorService.shutdownNow();
            }
        }
    }

    private void createSchemaEventsForTables(RelationalSnapshotContext<P, O> snapshotContext,
                                             Collection<TableId> tablesToRead,
                                             boolean firstPhase)
            throws Exception {
        List<TableId> realTablesToRead = new ArrayList<>(tablesToRead);
        if (firstPhase) {
            realTablesToRead = realTablesToRead.stream()
                    .filter(id -> !delayedSchemaSnapshotTables.contains(id))
                    .collect(Collectors.toList());
        }
        for (TableId tableId : realTablesToRead) {
            connection.query("SHOW CREATE TABLE " + connection.quotedTableIdString(tableId), rs -> {
                if (rs.next()) {
                    addSchemaEvent(snapshotContext, tableId.catalog(), rs.getString(2));
                }
            });
        }
    }

    private void createSchemaEventsForTables(RelationalSnapshotContext<P, O> snapshotContext,
                                             Collection<TableId> tablesToRead,
                                             boolean firstPhase,
                                             ExecutorService executorService)
            throws Exception {
        List<TableId> realTablesToRead = new ArrayList<>(tablesToRead);
        if (firstPhase) {
            realTablesToRead = realTablesToRead.stream()
                    .filter(id -> !delayedSchemaSnapshotTables.contains(id))
                    .collect(Collectors.toList());
        }
        if (!realTablesToRead.isEmpty()) {
            CompletionService<Map<TableId, String>> completionService = new ExecutorCompletionService<>(executorService);
            for (TableId tableId : realTablesToRead) {
                completionService.submit(createDdlForTableCallable(tableId, connectionPool));
            }
            Map<TableId, String> ddls = new HashMap<>();
            for (int i = 0; i < realTablesToRead.size(); i++) {
                Map<TableId, String> ddl = completionService.take().get();
                if (ddl != null) {
                    ddls.putAll(ddl);
                }
            }
            ddls.forEach((key, value) -> addSchemaEvent(snapshotContext, key.catalog(), value));
        }
    }

    private Callable<Map<TableId, String>> createDdlForTableCallable(TableId tableId, Queue<JdbcConnection> connectionPool) {
        return () -> {
            JdbcConnection connection = connectionPool.poll();
            assert connection != null;
            try {
                Map<TableId, String> result = new HashMap<>();
                connection.query("SHOW CREATE TABLE " + connection.quotedTableIdString(tableId), rs -> {
                    if (rs.next()) {
                        result.put(tableId, rs.getString(2));
                    }
                });
                return result;
            }
            finally {
                connectionPool.add(connection);
            }
        };
    }

    private boolean twoPhaseSchemaSnapshot() {
        if (!isGloballyLocked() && connectorConfig.getSnapshotLockingStrategy().preventsTableLocks()) {
            // Prevent obtaining individual table-level read locks
            // using 'FLUSH TABLE <tableName> WITH READ LOCK'
            // when using *_no_table_locks mode
            throw new DebeziumException(
                    "Cannot perform two-phase schema snapshot because global read lock was not acquired and table locks are not allowed in *_no_table_locks mode.");
        }
        return connectorConfig.getSnapshotLockingStrategy().isLockingEnabled() && !isGloballyLocked();
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<P, O> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                snapshotContext.catalogName,
                table);
    }

    /**
     * Generate a valid MySQL query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext,
                                                 TableId tableId,
                                                 List<String> columns) {
        return getSnapshotSelect(tableId, columns);
    }

    private Optional<String> getSnapshotSelect(TableId tableId, List<String> columns) {
        return snapshotterService.getSnapshotQuery().snapshotQuery(tableId.toQuotedString('`'), columns);
    }

    @Override
    protected Optional<String> getSnapshotConnectionFirstSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId) {
        if (getSnapshotSelect(tableId, List.of("*")).isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getSnapshotSelect(tableId, List.of("*")).get() + " LIMIT 1");
    }

    private boolean isGloballyLocked() {
        return globalLockAcquiredAt != -1;
    }

    private boolean isTablesLocked() {
        return tableLockAcquiredAt != -1;
    }

    private void globalLock() throws SQLException {
        LOGGER.info("Flush and obtain global read lock to prevent writes to database");
        Optional<String> lockingStatement = snapshotterService.getSnapshotLock().tableLockingStatement(null, null);
        if (lockingStatement.isPresent()) {
            connection.executeWithoutCommitting(lockingStatement.get());
            globalLockAcquiredAt = clock.currentTimeInMillis();
        }
    }

    private void globalUnlock() throws SQLException {
        LOGGER.info("Releasing global read lock to enable MySQL writes");
        connection.executeWithoutCommitting("UNLOCK TABLES");
        long lockReleased = clock.currentTimeInMillis();
        metrics.setGlobalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - globalLockAcquiredAt));
        globalLockAcquiredAt = -1;
    }

    private void tableLock(RelationalSnapshotContext<P, O> snapshotContext)
            throws SQLException {
        // ------------------------------------
        // LOCK TABLES and READ BINLOG POSITION
        // ------------------------------------
        // We were not able to acquire the global read lock, so instead we have to obtain a read lock on each table.
        // This requires different privileges than normal, and also means we can't unlock the tables without
        // implicitly committing our transaction ...
        if (!connection.userHasPrivileges("LOCK TABLES")) {
            // We don't have the right privileges
            throw new DebeziumException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                    + "consistent snapshot by preventing concurrent writes to tables.");
        }
        // We have the required privileges, so try to lock all of the tables we're interested in ...
        LOGGER.info("Flush and obtain read lock for {} tables (preventing writes)", snapshotContext.capturedTables);
        if (!snapshotContext.capturedTables.isEmpty()) {
            final String tableList = snapshotContext.capturedTables.stream()
                    .map(connection::quotedTableIdString)
                    .collect(Collectors.joining(","));
            connection.executeWithoutCommitting("FLUSH TABLES " + tableList + " WITH READ LOCK");
        }
        tableLockAcquiredAt = clock.currentTimeInMillis();
        metrics.setGlobalLockAcquired();
    }

    private void tableUnlock() throws SQLException {
        LOGGER.info("Releasing table read lock to enable MySQL writes");
        connection.executeWithoutCommitting("UNLOCK TABLES");
        long lockReleased = clock.currentTimeInMillis();
        metrics.setGlobalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - tableLockAcquiredAt));
        tableLockAcquiredAt = -1;
    }

    @Override
    protected OptionalLong rowCountForTable(TableId tableId) {
        if (getSnapshotSelectOverridesByTable(tableId, connectorConfig.getSnapshotSelectOverridesByTable()) != null) {
            return super.rowCountForTable(tableId);
        }
        if (ROW_ESTIMATE_LOGGER.isInfoEnabled() || connectorConfig.snapshotOrderByRowCount() != SnapshotTablesRowCountOrder.DISABLED) {
            OptionalLong rowCount = connection.getEstimatedTableSize(tableId);
            LOGGER.info("Estimated row count for table {} is {}", tableId, rowCount);
            return rowCount;
        }
        return OptionalLong.empty();
    }

    @Override
    protected Statement readTableStatement(JdbcConnection jdbcConnection, OptionalLong rowCount) throws SQLException {
        BinlogConnectorConnection connection = (BinlogConnectorConnection) jdbcConnection;
        final long largeTableRowCount = connectorConfig.getRowCountForLargeTable();
        if (rowCount.isEmpty() || largeTableRowCount == 0 || rowCount.getAsLong() <= largeTableRowCount) {
            return super.readTableStatement(connection, rowCount);
        }
        return createStatementWithLargeResultSet(connection);
    }

    /**
     * Create a JDBC statement that can be used for large result sets.
     * <p>
     * By default, the MySQL Connector/J driver retrieves all rows for ResultSets and stores them in memory. In most cases this
     * is the most efficient way to operate and, due to the design of the MySQL network protocol, is easier to implement.
     * However, when ResultSets that have a large number of rows or large values, the driver may not be able to allocate
     * heap space in the JVM and may result in an {@link OutOfMemoryError}. See
     * <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     * <p>
     * This method handles such cases using the
     * <a href="https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html">recommended
     * technique</a> for MySQL by creating the JDBC {@link Statement} with {@link ResultSet#TYPE_FORWARD_ONLY forward-only} cursor
     * and {@link ResultSet#CONCUR_READ_ONLY read-only concurrency} flags, and with a {@link Integer#MIN_VALUE minimum value}
     * {@link Statement#setFetchSize(int) fetch size hint}.
     *
     * @return the statement; never null
     * @throws SQLException if there is a problem creating the statement
     */
    private Statement createStatementWithLargeResultSet(BinlogConnectorConnection connection) throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        Statement stmt = connection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class BinlogSnapshotContext<P extends BinlogPartition, O extends BinlogOffsetContext>
            extends RelationalSnapshotContext<P, O> {
        BinlogSnapshotContext(P partition, boolean onDemand) {
            super(partition, "", onDemand);
        }
    }

    @Override
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<P, O> snapshotContext,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        tryStartingSnapshot(snapshotContext);

        for (final SchemaChangeEvent event : schemaEvents) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while processing event " + event);
            }

            if (databaseSchema.skipSchemaChangeEvent(event)) {
                continue;
            }

            LOGGER.debug("Processing schema event {}", event);

            final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
            if (snapshottingTask.isOnDemand() && !snapshotContext.capturedTables.contains(tableId)) {
                LOGGER.debug("Event {} will be skipped since it's not related to blocking snapshot captured table {}", event, snapshotContext.capturedTables);
                continue;
            }
            snapshotContext.offset.event(tableId, getClock().currentTime());
            dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, snapshotContext.offset, tableId, (receiver) -> receiver.schemaChangeEvent(event));
        }

        // Make schema available for snapshot source
        databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
    }

    @Override
    protected void postSnapshot() throws InterruptedException {
        // We cannot be sure that the last event as the last one
        // - last table could be empty
        // - data snapshot was not executed
        // - the last table schema snaphsotted is not monitored and storing of monitored is disabled
        lastEventProcessor.accept(record -> {
            record.sourceOffset().remove(BinlogSourceInfo.SNAPSHOT_KEY);
            ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).put(
                    BinlogSourceInfo.SNAPSHOT_KEY,
                    SnapshotRecord.LAST.toString().toLowerCase());
            return record;
        });
        super.postSnapshot();
    }

    @Override
    protected void preSnapshot() throws InterruptedException {
        preSnapshotAction.run();
        super.preSnapshot();
    }

    @Override
    protected void aborted(SnapshotContext<P, O> snapshotContext) throws InterruptedException {

        lastEventProcessor.accept(Function.identity());

        super.aborted(snapshotContext);
    }
}
