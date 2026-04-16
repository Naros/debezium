/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.pipeline.source.spi.ChangeTableResultSet;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.ColumnUtils;

/**
 * The logical representation of a position for the change in the transaction log.
 * During each sourcing cycle it is necessary to query all change tables and then
 * make a total order of changes across all tables.<br>
 * This class represents an open database cursor over the change table that is
 * able to move the cursor forward and report the LSN for the change to which the cursor
 * now points.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerChangeTablePointer extends ChangeTableResultSet<SqlServerChangeTable, TxLogPosition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerChangeTablePointer.class);

    private static final int INVALID_COLUMN_INDEX = -1;

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_UPDATE_MASK = 4;
    private static final int COL_DATA = 5;

    private ResultSetMapper<Object[]> resultSetMapper;
    private final int columnDataOffset;
    private final SqlServerConnection connection;
    private final Lsn fromLsn;
    private final Lsn toLsn;
    private final int maxRowsPerResultSet;

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, SqlServerConnection connection, Lsn fromLsn, Lsn toLsn, int maxRowsPerResultSet) {
        super(changeTable, COL_DATA, maxRowsPerResultSet);
        // Store references to these because we can't get them from our superclass
        this.columnDataOffset = COL_DATA;
        this.connection = connection;
        this.fromLsn = fromLsn;
        this.toLsn = toLsn;
        this.maxRowsPerResultSet = maxRowsPerResultSet;
    }

    @Override
    protected int getOperation(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    @Override
    protected Object getColumnData(ResultSet resultSet, int columnIndex) throws SQLException {
        if (resultSet.getMetaData().getColumnType(columnIndex) == Types.TIME) {
            return resultSet.getTimestamp(columnIndex);
        }
        return super.getColumnData(resultSet, columnIndex);
    }

    @Override
    protected TxLogPosition getNextChangePosition(ResultSet resultSet) throws SQLException {
        return isCompleted() ? TxLogPosition.NULL
                : TxLogPosition.valueOf(
                        Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)),
                        Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)),
                        resultSet.getInt(COL_OPERATION));
    }

    /**
     * Check whether TX in currentChangePosition is newer (higher) than TX in previousChangePosition
     * @return true <=> TX in currentChangePosition > TX in previousChangePosition
     * @throws SQLException
     */
    protected boolean isNewTransaction() throws SQLException {
        return (getPreviousChangePosition() != null) &&
                getChangePosition().getCommitLsn().compareTo(getPreviousChangePosition().getCommitLsn()) > 0;
    }

    @Override
    protected ResultSet getNextResultSet(TxLogPosition lastPositionSeen) throws SQLException {
        if (lastPositionSeen == null || lastPositionSeen.equals(TxLogPosition.NULL)) {
            return connection.getChangesForTable(getChangeTable(), fromLsn, toLsn, maxRowsPerResultSet);
        }
        else {
            return connection.getChangesForTable(getChangeTable(), lastPositionSeen.getCommitLsn(), lastPositionSeen.getInTxLsn(), lastPositionSeen.getOperation(),
                    toLsn, maxRowsPerResultSet);
        }
    }

    @Override
    public Object[] getData() throws SQLException {
        if (resultSetMapper == null) {
            this.resultSetMapper = createResultSetMapper(getChangeTable().getSourceTable());
        }
        return resultSetMapper.apply(getResultSet());
    }

    /**
     * Internally each row is represented as an array of objects, where the order of values
     * corresponds to the order of columns (fields) in the table schema. However, when capture
     * instance contains only a subset of original's table column, in order to preserve the
     * aforementioned order of values in array, raw database results have to be adjusted
     * accordingly.
     *
     * @param table original table
     * @return a mapper which adjusts order of values in case the capture instance contains only
     * a subset of columns
     */
    private ResultSetMapper<Object[]> createResultSetMapper(Table table) throws SQLException {
        ColumnUtils.MappedColumns columnMap = ColumnUtils.toMap(table);
        final ResultSetMetaData rsmd = getResultSet().getMetaData();
        final int columnCount = rsmd.getColumnCount() - columnDataOffset;
        final List<String> resultColumns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            resultColumns.add(rsmd.getColumnName(columnDataOffset + i));
        }
        final int resultColumnCount = resultColumns.size();

        // Identify which columns are MAX type columns (VARBINARY(MAX), VARCHAR(MAX), NVARCHAR(MAX)).
        // These appear as NULL in the CDC capture table when not modified in an UPDATE, and need to
        // be replaced with the UNAVAILABLE_VALUE sentinel so they can be exposed via the
        // unavailable.value.placeholder configuration.
        final boolean[] isMaxColumn = new boolean[resultColumnCount];
        for (int i = 0; i < resultColumnCount; i++) {
            final Column column = columnMap.getSourceTableColumns().get(resultColumns.get(i));
            if (column != null) {
                final int jdbcType = column.jdbcType();
                isMaxColumn[i] = (jdbcType == Types.LONGVARBINARY
                        || jdbcType == Types.LONGVARCHAR
                        || jdbcType == Types.LONGNVARCHAR);
            }
        }

        final IndicesMapping indicesMapping = new IndicesMapping(columnMap.getSourceTableColumns(), resultColumns);
        return resultSet -> {
            final Object[] data = new Object[columnMap.getGreatestColumnPosition()];

            // For UPDATE operations, read the update mask to detect unchanged MAX columns.
            // The __$update_mask bitmask has a bit set for each column that was modified;
            // MAX columns with their bit unset were not changed and have NULL in the CDC table.
            final int operation = resultSet.getInt(COL_OPERATION);
            final byte[] updateMask;
            if (operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE
                    || operation == SqlServerChangeRecordEmitter.OP_UPDATE_AFTER) {
                updateMask = resultSet.getBytes(COL_UPDATE_MASK);
            }
            else {
                updateMask = null;
            }

            for (int i = 0; i < resultColumnCount; i++) {
                int index = indicesMapping.getSourceTableColumnIndex(i);
                if (index == INVALID_COLUMN_INDEX) {
                    LOGGER.trace("Data for table '{}' contains a column without position mapping", table.id());
                    continue;
                }
                Object value = getColumnData(resultSet, columnDataOffset + i);

                // If this is a MAX column in an UPDATE operation and its bit is not set in the
                // update mask, the column was not changed. Replace NULL with UNAVAILABLE_VALUE to
                // distinguish it from a column that was explicitly set to NULL.
                if (updateMask != null && isMaxColumn[i] && value == null
                        && !isColumnInUpdateMask(updateMask, i)) {
                    value = SqlServerValueConverters.UNAVAILABLE_VALUE;
                }

                data[index] = value;
            }
            return data;
        };
    }

    /**
     * Checks whether the column at the given 0-based index within the capture instance has
     * its bit set in the SQL Server CDC {@code __$update_mask}.
     *
     * <p>The update mask uses one bit per captured column, ordered by column ordinal (LSB first).
     * A set bit indicates the column was modified in the UPDATE operation.
     *
     * @param updateMask the raw bytes of {@code __$update_mask}; may be null
     * @param columnIndex the 0-based index of the column within the capture instance
     * @return {@code true} if the column's bit is set, {@code false} otherwise
     */
    private static boolean isColumnInUpdateMask(byte[] updateMask, int columnIndex) {
        if (updateMask == null || updateMask.length == 0) {
            return false;
        }
        final int byteIndex = columnIndex / 8;
        final int bitIndex = columnIndex % 8;
        if (byteIndex >= updateMask.length) {
            return false;
        }
        return (updateMask[byteIndex] & (1 << bitIndex)) != 0;
    }

    private class IndicesMapping {

        private final Map<Integer, Integer> mapping;

        IndicesMapping(Map<String, Column> sourceTableColumns, List<String> captureInstanceColumns) {
            this.mapping = new HashMap<>(sourceTableColumns.size(), 1.0F);

            for (int i = 0; i < captureInstanceColumns.size(); ++i) {
                final String columnName = captureInstanceColumns.get(i);
                final Column column = sourceTableColumns.get(columnName);
                if (column == null) {
                    LOGGER.warn("Column '{}' available in capture table not found among source table columns", columnName);
                    mapping.put(i, INVALID_COLUMN_INDEX);
                }
                else {
                    mapping.put(i, column.position() - 1);
                }
            }

        }

        int getSourceTableColumnIndex(int resultCaptureInstanceColumnIndex) {
            return mapping.get(resultCaptureInstanceColumnIndex);
        }
    }

}
