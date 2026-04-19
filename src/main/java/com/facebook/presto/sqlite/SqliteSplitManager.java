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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqliteSplitManager
        implements ConnectorSplitManager
{
    private static final int SPLIT_TARGET_COUNT = 4;
    private static final long MIN_ROWS_PER_SPLIT = 100_000;

    private final SqliteClient sqliteClient;

    public SqliteSplitManager(SqliteClient sqliteClient)
    {
        this.sqliteClient = sqliteClient;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        SqliteTableLayoutHandle layoutHandle = (SqliteTableLayoutHandle) layout;
        SqliteTableHandle tableHandle = layoutHandle.getTable();
        String whereClause = layoutHandle.getWhereClause();
        String schema = tableHandle.getSchemaName();
        String table = tableHandle.getTableName();

        long minRowid = -1;
        long maxRowid = -1;

        try (Connection conn = sqliteClient.getConnection();
             Statement stmt = conn.createStatement()) {
            String sql = "SELECT MIN(ROWID), MAX(ROWID) FROM \"" + table + "\"";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    minRowid = rs.getLong(1);
                    if (rs.wasNull()) {
                        minRowid = -1;
                    }
                    maxRowid = rs.getLong(2);
                    if (rs.wasNull()) {
                        maxRowid = -1;
                    }
                }
            }
        }
        catch (SQLException e) {
            // Fall back to single split if ROWID query fails (e.g., views)
            ConnectorSplit single = new SqliteSplit(schema, table, whereClause, -1, -1);
            return new FixedSplitSource(ImmutableList.of(single));
        }

        long rowidRange = maxRowid - minRowid + 1;
        if (minRowid < 0 || maxRowid < 0 || rowidRange < MIN_ROWS_PER_SPLIT * 2) {
            ConnectorSplit single = new SqliteSplit(schema, table, whereClause, -1, -1);
            return new FixedSplitSource(ImmutableList.of(single));
        }

        int splitCount = (int) Math.min(SPLIT_TARGET_COUNT, rowidRange / MIN_ROWS_PER_SPLIT);
        splitCount = Math.max(splitCount, 2);

        long rangePerSplit = rowidRange / splitCount;
        List<ConnectorSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            long start = minRowid + (i * rangePerSplit);
            long end = (i == splitCount - 1) ? maxRowid : (start + rangePerSplit - 1);
            splits.add(new SqliteSplit(schema, table, whereClause, start, end));
        }

        return new FixedSplitSource(ImmutableList.copyOf(splits));
    }
}
