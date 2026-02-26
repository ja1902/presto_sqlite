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

import com.facebook.presto.spi.PrestoException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Manages JDBC connections to a SQLite database file.
 */
public class SqliteClient
{
    private final String jdbcUrl;

    public SqliteClient(String dbPath)
    {
        requireNonNull(dbPath, "dbPath is null");
        this.jdbcUrl = "jdbc:sqlite:" + dbPath;

        // Ensure the SQLite JDBC driver is loaded
        try {
            Class.forName("org.sqlite.JDBC");
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "SQLite JDBC driver not found", e);
        }
    }

    public Connection getConnection()
    {
        try {
            return DriverManager.getConnection(jdbcUrl);
        }
        catch (SQLException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to connect to SQLite database: " + e.getMessage(), e);
        }
    }
}
