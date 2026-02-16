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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SqliteConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "sqlite";

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new SqliteHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        String dbPath = config.get("sqlite.db");
        if (dbPath == null) {
            throw new IllegalArgumentException("sqlite.db configuration property is required. " +
                    "Set it to the path of your SQLite database file.");
        }

        SqliteClient sqliteClient = new SqliteClient(dbPath);

        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return SqliteTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new SqliteMetadata(sqliteClient);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new SqliteSplitManager();
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new SqliteRecordSetProvider(sqliteClient);
            }
        };
    }
}
