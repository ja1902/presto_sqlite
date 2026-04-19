/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sqlite;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class SqliteRecordSet
        implements RecordSet
{
    private final SqliteClient sqliteClient;
    private final String tableName;
    private final List<SqliteColumnHandle> columns;
    private final List<Type> columnTypes;
    private final String whereClause;
    private final long rowidStart;
    private final long rowidEnd;

    public SqliteRecordSet(SqliteClient sqliteClient, String tableName, List<SqliteColumnHandle> columns,
            String whereClause, long rowidStart, long rowidEnd)
    {
        this.sqliteClient = requireNonNull(sqliteClient, "sqliteClient is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.columnTypes = columns.stream()
                .map(SqliteColumnHandle::getType)
                .collect(Collectors.toList());
        this.whereClause = whereClause == null ? "" : whereClause;
        this.rowidStart = rowidStart;
        this.rowidEnd = rowidEnd;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new SqliteRecordCursor(sqliteClient, tableName, columns, whereClause, rowidStart, rowidEnd);
    }

    private static String buildWhereFragment(String whereClause, long rowidStart, long rowidEnd)
    {
        List<String> conditions = new ArrayList<>();
        if (whereClause != null && !whereClause.isEmpty()) {
            conditions.add(whereClause);
        }
        if (rowidStart >= 0 && rowidEnd >= 0) {
            conditions.add("ROWID BETWEEN " + rowidStart + " AND " + rowidEnd);
        }
        if (conditions.isEmpty()) {
            return "";
        }
        return " WHERE " + String.join(" AND ", conditions);
    }

    public static class SqliteRecordCursor
            implements RecordCursor
    {
        private final List<SqliteColumnHandle> columns;
        private final Connection connection;
        private final Statement statement;
        private final ResultSet resultSet;
        private boolean closed;
        private long completedBytes;
        private final boolean countMode;
        private long countRemaining;

        public SqliteRecordCursor(SqliteClient sqliteClient, String tableName, List<SqliteColumnHandle> columns,
                String whereClause, long rowidStart, long rowidEnd)
        {
            this.columns = requireNonNull(columns, "columns is null");

            try {
                this.connection = sqliteClient.getConnection();
                this.statement = connection.createStatement();
                this.statement.setFetchSize(10000);

                String whereFragment = buildWhereFragment(whereClause, rowidStart, rowidEnd);

                if (columns.isEmpty()) {
                    // COUNT(*) optimization: get the count from SQLite and use a counter
                    String countSql = "SELECT COUNT(*) FROM \"" + tableName + "\"" + whereFragment;
                    ResultSet countRs = statement.executeQuery(countSql);
                    if (countRs.next()) {
                        this.countRemaining = countRs.getLong(1);
                    }
                    else {
                        this.countRemaining = 0;
                    }
                    countRs.close();
                    this.countMode = true;
                    this.resultSet = null;
                }
                else {
                    String columnList = columns.stream()
                            .map(col -> "\"" + col.getColumnName() + "\"")
                            .collect(Collectors.joining(", "));
                    String sql = "SELECT " + columnList + " FROM \"" + tableName + "\"" + whereFragment;
                    this.resultSet = statement.executeQuery(sql);
                    this.countMode = false;
                    this.countRemaining = 0;
                }
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to execute SQLite query: " + e.getMessage(), e);
            }
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return columns.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (closed) {
                return false;
            }

            if (countMode) {
                if (countRemaining > 0) {
                    countRemaining--;
                    return true;
                }
                close();
                return false;
            }

            try {
                boolean hasNext = resultSet.next();
                if (!hasNext) {
                    close();
                }
                return hasNext;
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to advance cursor: " + e.getMessage(), e);
            }
        }

        @Override
        public boolean getBoolean(int field)
        {
            try {
                return resultSet.getBoolean(field + 1);
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read boolean: " + e.getMessage(), e);
            }
        }

        @Override
        public long getLong(int field)
        {
            try {
                long value = resultSet.getLong(field + 1);
                completedBytes += Long.BYTES;
                return value;
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read long: " + e.getMessage(), e);
            }
        }

        @Override
        public double getDouble(int field)
        {
            try {
                double value = resultSet.getDouble(field + 1);
                completedBytes += Double.BYTES;
                return value;
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read double: " + e.getMessage(), e);
            }
        }

        @Override
        public Slice getSlice(int field)
        {
            try {
                String value = resultSet.getString(field + 1);
                if (value == null) {
                    return Slices.EMPTY_SLICE;
                }
                Slice slice = Slices.utf8Slice(value);
                completedBytes += slice.length();
                return slice;
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read string: " + e.getMessage(), e);
            }
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException("getObject not supported");
        }

        @Override
        public boolean isNull(int field)
        {
            try {
                resultSet.getObject(field + 1);
                return resultSet.wasNull();
            }
            catch (SQLException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to check null: " + e.getMessage(), e);
            }
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    }
                    catch (SQLException ignored) {
                    }
                }
                try {
                    statement.close();
                }
                catch (SQLException ignored) {
                }
                try {
                    connection.close();
                }
                catch (SQLException ignored) {
                }
            }
        }
    }
}
