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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqliteRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final SqliteClient sqliteClient;

    public SqliteRecordSetProvider(SqliteClient sqliteClient)
    {
        this.sqliteClient = requireNonNull(sqliteClient, "sqliteClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        SqliteSplit sqliteSplit = (SqliteSplit) split;

        ImmutableList.Builder<SqliteColumnHandle> builder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            builder.add((SqliteColumnHandle) column);
        }

        return new SqliteRecordSet(sqliteClient, sqliteSplit.getTableName(), builder.build());
    }
}
