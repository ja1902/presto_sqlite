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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class SqliteMetadata
        implements ConnectorMetadata
{
    public static final String DEFAULT_SCHEMA = "default";

    private final SqliteClient sqliteClient;

    public SqliteMetadata(SqliteClient sqliteClient)
    {
        this.sqliteClient = requireNonNull(sqliteClient, "sqliteClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");

        if (!DEFAULT_SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }

        try (Connection connection = sqliteClient.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables = metadata.getTables(null, null, tableName.getTableName(), new String[] {"TABLE", "VIEW"})) {
                if (tables.next()) {
                    return new SqliteTableHandle(DEFAULT_SCHEMA, tableName.getTableName());
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get table handle: " + e.getMessage(), e);
        }

        return null;
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        SqliteTableHandle tableHandle = (SqliteTableHandle) table;
        TupleDomain<ColumnHandle> tupleDomain = constraint.getSummary();
        String whereClause = buildWhereClause(tupleDomain);
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new SqliteTableLayoutHandle(tableHandle, whereClause));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        SqliteTableHandle sqliteTableHandle = (SqliteTableHandle) table;
        List<ColumnMetadata> columns = getColumnsMetadata(sqliteTableHandle.getTableName());
        return new ConnectorTableMetadata(
                new SchemaTableName(sqliteTableHandle.getSchemaName(), sqliteTableHandle.getTableName()),
                columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SqliteTableHandle sqliteTableHandle = (SqliteTableHandle) tableHandle;
        List<ColumnMetadata> columns = getColumnsMetadata(sqliteTableHandle.getTableName());

        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        int ordinalPosition = 0;
        for (ColumnMetadata column : columns) {
            builder.put(column.getName(), new SqliteColumnHandle(column.getName(), column.getType(), ordinalPosition));
            ordinalPosition++;
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        SqliteColumnHandle sqliteColumnHandle = (SqliteColumnHandle) columnHandle;
        return ColumnMetadata.builder().setName(sqliteColumnHandle.getColumnName()).setType(sqliteColumnHandle.getType()).build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, Optional.ofNullable(prefix.getSchemaName()))) {
            if (prefix.getTableName() == null || tableName.getTableName().equals(prefix.getTableName())) {
                columns.put(tableName, getColumnsMetadata(tableName.getTableName()));
            }
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !DEFAULT_SCHEMA.equals(schemaName.get())) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        try (Connection connection = sqliteClient.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet rs = metadata.getTables(null, null, "%", new String[] {"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    tables.add(new SchemaTableName(DEFAULT_SCHEMA, tableName));
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to list tables: " + e.getMessage(), e);
        }
        return tables.build();
    }

    private List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        List<ColumnMetadata> columns = new ArrayList<>();
        try (Connection connection = sqliteClient.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet rs = metadata.getColumns(null, null, tableName, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME").toUpperCase();
                    int sqlType = rs.getInt("DATA_TYPE");
                    Type prestoType = sqliteTypeToPrestoType(typeName, sqlType);
                    columns.add(ColumnMetadata.builder().setName(columnName).setType(prestoType).build());
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get columns for table " + tableName + ": " + e.getMessage(), e);
        }
        return columns;
    }

    static Type sqliteTypeToPrestoType(String typeName, int sqlType)
    {
        // SQLite has flexible typing; map by declared type name and JDBC type
        if (typeName.contains("INT")) {
            if (typeName.contains("BIGINT")) {
                return BIGINT;
            }
            return INTEGER;
        }
        if (typeName.contains("REAL") || typeName.contains("FLOAT") || typeName.contains("DOUBLE")) {
            return DOUBLE;
        }
        if (typeName.contains("BOOL")) {
            return BOOLEAN;
        }
        if (typeName.contains("CHAR") || typeName.contains("TEXT") || typeName.contains("CLOB") || typeName.contains("VARCHAR")) {
            return VARCHAR;
        }
        if (typeName.contains("BLOB") || typeName.isEmpty()) {
            return VARCHAR;
        }
        if (typeName.contains("NUMERIC") || typeName.contains("DECIMAL")) {
            return DOUBLE;
        }

        // Fall back based on JDBC SQL type
        switch (sqlType) {
            case java.sql.Types.BIGINT:
                return BIGINT;
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                return INTEGER;
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.REAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return DOUBLE;
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return BOOLEAN;
            default:
                return VARCHAR;
        }
    }

    // --- Predicate pushdown: TupleDomain to SQL WHERE clause ---

    static String buildWhereClause(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return "";
        }
        if (tupleDomain.isNone()) {
            return "1 = 0";
        }

        Optional<Map<ColumnHandle, Domain>> domains = tupleDomain.getDomains();
        if (!domains.isPresent()) {
            return "";
        }

        List<String> conjuncts = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            SqliteColumnHandle column = (SqliteColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            String fragment = domainToSql(column, domain);
            if (fragment != null) {
                conjuncts.add(fragment);
            }
        }

        if (conjuncts.isEmpty()) {
            return "";
        }
        return String.join(" AND ", conjuncts);
    }

    private static String domainToSql(SqliteColumnHandle column, Domain domain)
    {
        if (domain.isAll()) {
            return null;
        }
        if (domain.isNone()) {
            return "1 = 0";
        }

        String quotedName = "\"" + column.getColumnName() + "\"";

        if (domain.isSingleValue()) {
            return quotedName + " = " + valueToLiteral(domain.getSingleValue());
        }

        ValueSet values = domain.getValues();
        boolean nullAllowed = domain.isNullAllowed();

        List<String> disjuncts = new ArrayList<>();

        if (values instanceof SortedRangeSet) {
            SortedRangeSet rangeSet = (SortedRangeSet) values;
            List<Range> ranges = rangeSet.getOrderedRanges();

            List<Object> equalities = new ArrayList<>();
            List<String> rangeSql = new ArrayList<>();

            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    equalities.add(range.getSingleValue());
                }
                else {
                    String r = rangeToSql(quotedName, range);
                    if (r != null) {
                        rangeSql.add(r);
                    }
                }
            }

            if (!equalities.isEmpty()) {
                if (equalities.size() == 1) {
                    disjuncts.add(quotedName + " = " + valueToLiteral(equalities.get(0)));
                }
                else {
                    String inList = equalities.stream()
                            .map(SqliteMetadata::valueToLiteral)
                            .collect(Collectors.joining(", "));
                    disjuncts.add(quotedName + " IN (" + inList + ")");
                }
            }
            disjuncts.addAll(rangeSql);
        }
        else {
            // AllOrNoneValueSet or EquatableValueSet: skip pushdown for this column
            return null;
        }

        if (nullAllowed) {
            disjuncts.add(quotedName + " IS NULL");
        }

        if (disjuncts.isEmpty()) {
            return null;
        }
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }
        return "(" + String.join(" OR ", disjuncts) + ")";
    }

    private static String rangeToSql(String quotedName, Range range)
    {
        if (range.isAll()) {
            return null;
        }

        List<String> parts = new ArrayList<>();

        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case ABOVE:
                    parts.add(quotedName + " > " + valueToLiteral(range.getLow().getValue()));
                    break;
                case EXACTLY:
                    parts.add(quotedName + " >= " + valueToLiteral(range.getLow().getValue()));
                    break;
                case BELOW:
                    break;
            }
        }

        if (!range.getHigh().isUpperUnbounded()) {
            switch (range.getHigh().getBound()) {
                case BELOW:
                    parts.add(quotedName + " < " + valueToLiteral(range.getHigh().getValue()));
                    break;
                case EXACTLY:
                    parts.add(quotedName + " <= " + valueToLiteral(range.getHigh().getValue()));
                    break;
                case ABOVE:
                    break;
            }
        }

        if (parts.isEmpty()) {
            return null;
        }
        return String.join(" AND ", parts);
    }

    static String valueToLiteral(Object value)
    {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Slice) {
            String s = ((Slice) value).toStringUtf8();
            return "'" + s.replace("'", "''") + "'";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "1" : "0";
        }
        return String.valueOf(value);
    }
}
