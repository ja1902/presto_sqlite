# presto-sqlite

A [Presto](https://prestodb.io/) connector for [SQLite](https://www.sqlite.org/) databases.
Query `.db` / `.sqlite` files directly from Presto using standard SQL.

Read-only. Single schema (`default`). Built against Presto **0.296**.

## Requirements

- Java 8+ (for building)
- Maven 3.6+
- A running Presto installation

## Setup

Clone the repo and run the install script. It handles everything:

1. Builds the plugin from source
2. Copies the JARs into Presto's plugin directory
3. Creates the catalog properties file
4. Sets up a Python virtual environment with `prestodb`
5. Creates the demo SQLite database

**Linux / macOS:**

```sh
git clone https://github.com/ja1902/presto-sqlite.git
cd presto-sqlite
chmod +x install.sh
./install.sh /opt/presto /absolute/path/to/your/database.db
```

**Windows (PowerShell):**

```powershell
git clone https://github.com/ja1902/presto-sqlite.git
cd presto-sqlite
.\install.ps1 -PrestoHome C:\presto -SqliteDb C:\data\mydb.sqlite
```

Then restart Presto and query:

```sql
SHOW CATALOGS;                         -- should list "sqlite"
SHOW TABLES FROM sqlite."default";
SELECT * FROM sqlite."default".my_table;
```

All tables live under the `default` schema.

## Targeting a different Presto version

The `pom.xml` pins Presto dependencies to `0.296` (the latest release on Maven Central).
To build against a different version, override the property:

```sh
mvn clean package -Ddep.presto.version=0.295
```

This works for any Presto version whose SPI artifacts are published to Maven Central.

## Type mapping

| SQLite declared type                       | Presto type |
|-------------------------------------------|-------------|
| `INTEGER`, `INT`, `SMALLINT`, `TINYINT`   | `INTEGER`   |
| `BIGINT`                                  | `BIGINT`    |
| `REAL`, `FLOAT`, `DOUBLE`                 | `DOUBLE`    |
| `NUMERIC`, `DECIMAL`                      | `DOUBLE`    |
| `BOOLEAN`, `BOOL`                         | `BOOLEAN`   |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB`         | `VARCHAR`   |
| `BLOB`, empty, or anything else           | `VARCHAR`   |

SQLite uses dynamic typing, so the connector inspects declared column types
and falls back to `VARCHAR` for anything it can't map.

## Limitations

- Read-only (no `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`)
- Single schema (`default`)
- No predicate pushdown -- all filtering happens in Presto
- No parallel reads -- one split per table
- `DATE` and `TIMESTAMP` columns are returned as `VARCHAR`

## Demo

The install script creates a demo database and a Python venv with `prestodb`.
After restarting Presto, run the demo queries:

```sh
.venv/bin/python demo/query_presto.py          # Linux/macOS
.venv\Scripts\python.exe demo\query_presto.py   # Windows
```

## Project layout

```
presto-sqlite/
  pom.xml
  install.sh                        Setup script (Linux/macOS)
  install.ps1                       Setup script (Windows)
  src/
    assembly/
      plugin.xml                    Assembly descriptor for plugin packaging
    main/java/.../sqlite/
      SqlitePlugin.java             SPI entry point
      SqliteConnectorFactory.java   Creates connectors from catalog config
      SqliteMetadata.java           Schema/table/column metadata + type mapping
      SqliteSplitManager.java       Single split per table
      SqliteRecordSetProvider.java  Bridges splits to record sets
      SqliteRecordSet.java          RecordSet + RecordCursor (reads via JDBC)
      SqliteColumnHandle.java       Column handle (name, type, ordinal)
      SqliteTableHandle.java        Table handle (schema, table name)
      SqliteTableLayoutHandle.java  Layout wrapper
      SqliteSplit.java              Split definition
      SqliteHandleResolver.java     Handle class resolution
      SqliteTransactionHandle.java  Transaction handle (singleton)
    main/resources/META-INF/services/
      com.facebook.presto.spi.Plugin
  demo/
    create_sqlite_db.py             Creates a sample SQLite database
    query_presto.py                 Python script to query via Presto
    query_all.sql                   SQL queries for the Presto CLI
```

## License

Apache License 2.0
