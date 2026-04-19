# presto-sqlite

A custom [Presto](https://prestodb.io/) connector that lets you query **SQLite
`.db` files** as a first-class data source alongside any other database Presto
supports -- PostgreSQL, MySQL, Hive, Kafka, and more.

## What this is

[Presto](https://prestodb.io/) is a distributed SQL query engine designed for
federated analytics: it lets you **query multiple, completely different databases
with a single SQL statement**. Each data source is registered as a *catalog*.
Once registered, you can JOIN tables across them as if they lived in the same
database.

```sql
-- Example: join an internal SQLite file against a production PostgreSQL database
SELECT
    c.name          AS customer,
    p.name          AS product,
    co.total_amount
FROM postgres.public.customer_orders co          -- lives in PostgreSQL
JOIN postgres.public.customers       c  ON co.customer_id = c.id
JOIN sqlite."default".products       p  ON co.product_id  = p.id   -- lives in a .db file
ORDER BY co.total_amount DESC;
```

Presto ships with built-in connectors for PostgreSQL, MySQL, Hive, Kafka,
Cassandra, and many others. This project adds a **SQLite connector** so legacy
`.db` files, local analytical databases, or any SQLite-backed application can
participate in the same federated queries without any ETL.

### Using it in your own application

The setup here is not demo-only. Once the connector plugin is built and deployed
you can:

- Point the `sqlite` catalog at **any** `.db` / `.sqlite` file -- swap it any
  time by editing `etc/catalog/sqlite.properties` and restarting Presto.
- Add **as many catalogs as you need** (one `.properties` file per source) and
  query across all of them.
- Replace the sample PostgreSQL container with your **real** production
  PostgreSQL server -- just update `connection-url` in
  `etc/catalog/postgres.properties`.
- Connect your application to Presto on `localhost:8080` via any standard
  **JDBC / ODBC driver or REST client** (Python, Java, Node.js, and BI tools
  like Tableau or Metabase all work out of the box).

The install scripts are a convenience to get everything running locally.
For production deployments see the
[Presto documentation](https://prestodb.io/docs/current/).

## Performance

The connector includes several optimizations for handling large datasets:

| Optimization | Description | Impact |
|---|---|---|
| **Predicate pushdown** | WHERE clause filters are translated to SQL and executed inside SQLite, reducing data transfer | Up to **6x faster** on filtered joins |
| **Multi-split parallelism** | Large tables are split into ROWID ranges so Presto reads them in parallel | Up to **3x faster** on aggregations |
| **COUNT(\*) pushdown** | `SELECT COUNT(*)` runs natively in SQLite instead of streaming every row | ~**1.2x faster** |
| **Connection pooling** | HikariCP pool reuses JDBC connections across splits and queries | Reduced per-query overhead |

These were benchmarked against the 21 GB [PUDL](https://catalyst.coop/pudl/) energy
database (343 tables, largest 3.3M rows). See
[BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) for the full analysis, methodology,
and before/after timings.

To reproduce the benchmark yourself, see
[Running the PUDL benchmark](#running-the-pudl-benchmark) below.

---

## Requirements

| | Linux / macOS | Windows |
|---|---|---|
| Java 17+ | Yes (build only) | Yes (build only) |
| Python 3 | Yes | Yes |
| [Docker Desktop](https://www.docker.com/products/docker-desktop) | No | Yes |

Presto does not run natively on Windows, so the Windows script runs it in
Docker. Make sure Docker Desktop is **running** before executing `install.ps1`.

---

## Quick start

**Linux / macOS:**

```sh
git clone https://github.com/ja1902/presto_sqlite.git
cd presto_sqlite
chmod +x install.sh
./install.sh
```

**Windows (PowerShell):**

```powershell
git clone https://github.com/ja1902/presto_sqlite.git
cd presto_sqlite
powershell -ExecutionPolicy Bypass -File install.ps1
```

The script detects your OS automatically and asks two questions:

> **Do you already have Presto installed?** *(Linux / macOS only)*
> - **Yes** -- provide the path to your existing Presto installation.
> - **No** -- the script downloads and configures Presto for you.

> **Do you already have a SQLite database?**
> - **Yes** -- provide the absolute path to your `.db` / `.sqlite` file.
> - **No** -- a sample database is created automatically for the demo.

It then builds the plugin, writes catalog configs, sets up a Python virtual
environment, and starts Presto (natively on Linux/macOS; via Docker on Windows).

---

## Starting Presto

The install script starts Presto on the first run. For subsequent sessions:

**Linux / macOS:**

```sh
<presto-home>/bin/launcher start   # start in background
<presto-home>/bin/launcher stop    # stop
<presto-home>/bin/launcher run     # start in foreground (useful for debugging)
```

Replace `<presto-home>` with your Presto installation path (default:
`presto-server-0.296` inside the repo directory).

**Windows (PowerShell):**

```powershell
docker start presto-postgres presto   # start PostgreSQL then Presto
docker stop  presto presto-postgres   # stop both
```

Presto takes ~30-60 seconds to become ready:

```sh
curl http://localhost:8080/v1/info
```

---

## Demo

The install script creates two sample databases to demonstrate cross-catalog
queries. **These are purely for experimentation** -- once you've seen them work
you can point the catalogs at any real database.

| Catalog | Engine | Sample data |
|---|---|---|
| `sqlite` | SQLite file | `departments`, `employees`, `products`, `orders` |
| `postgres` | PostgreSQL | `customers`, `customer_orders` |

`customer_orders.product_id` intentionally references `products.id` in the
SQLite file, which is the cross-catalog join Presto resolves without any ETL.

Activate the venv and run the demo queries:

```sh
# Linux / macOS
source .venv/bin/activate
python demo/query_presto.py

# Windows
.\.venv\Scripts\Activate.ps1
python demo\query_presto.py
```

The script runs SQLite-only queries, PostgreSQL-only queries, and
**cross-catalog joins** that pull from both sources in the same `SELECT`.

---

## Connecting your own data sources

**Point the SQLite catalog at a real file** -- edit
`etc/catalog/sqlite.properties`:

```properties
connector.name=sqlite
sqlite.db=/absolute/path/to/your/database.db
```

**Connect to a real PostgreSQL server** -- edit
`etc/catalog/postgres.properties`:

```properties
connector.name=postgresql
connection-url=jdbc:postgresql://<host>:<port>/<database>
connection-user=<user>
connection-password=<password>
```

**Add more data sources** -- create any additional `.properties` file in
`etc/catalog/`. Presto loads every file there as a catalog on startup.
Built-in connector names include: `postgresql`, `mysql`, `mongodb`, `kafka`,
`hive`, `iceberg`, `cassandra`, `redis`, `elasticsearch`, and many more.

Restart Presto after any catalog change.

---

## Querying Presto

**Python:**

```python
import prestodb
conn = prestodb.dbapi.connect(host="localhost", port=8080, user="me")
cur  = conn.cursor()
cur.execute('SELECT * FROM sqlite."default".my_table')
rows = cur.fetchall()
```

**Presto CLI:**

```sh
presto --server localhost:8080 --catalog sqlite --schema default
presto> SHOW CATALOGS;
presto> SHOW TABLES FROM sqlite."default";
presto> SELECT * FROM sqlite."default".my_table;
```

All SQLite tables live under the `default` schema.

---

## Targeting a different Presto version

```sh
mvn clean package -Ddep.presto.version=0.295
```

Works for any version whose SPI artifacts are published to Maven Central.
The `pom.xml` defaults to `0.296`.

---

## SQLite type mapping

| SQLite declared type | Presto type |
|---|---|
| `INTEGER`, `INT`, `SMALLINT`, `TINYINT` | `INTEGER` |
| `BIGINT` | `BIGINT` |
| `REAL`, `FLOAT`, `DOUBLE` | `DOUBLE` |
| `NUMERIC`, `DECIMAL` | `DOUBLE` |
| `BOOLEAN`, `BOOL` | `BOOLEAN` |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB` | `VARCHAR` |
| `BLOB`, empty, or anything else | `VARCHAR` |

SQLite uses dynamic typing; the connector inspects declared column types and
falls back to `VARCHAR` for anything unmapped.

---

## SQLite connector limitations

- **Read-only** -- no `INSERT`, `UPDATE`, `DELETE`, or DDL
- **Single schema** (`default`)
- `DATE` / `TIMESTAMP` columns are returned as `VARCHAR`

---

## Running the PUDL benchmark

The [PUDL](https://catalyst.coop/pudl/) (Public Utility Data Liberation) database
is a 21 GB SQLite file containing US energy sector data from the EIA -- 343 tables,
with the largest at 3.3 million rows. It makes an excellent real-world stress test
for the connector.

A single script downloads the database, builds the connector, starts all services,
and runs both the SQLite-only benchmark and the cross-connector federation test
(SQLite + PostgreSQL).

**Prerequisites:** Java 8+, Docker Desktop (running), Python 3, [AWS CLI v2](https://aws.amazon.com/cli/)

**Windows (PowerShell):**

```powershell
powershell -ExecutionPolicy Bypass -File run_pudl_benchmark.ps1
```

**Linux / macOS:**

```sh
chmod +x run_pudl_benchmark.sh
./run_pudl_benchmark.sh
```

The script is fully automated (no prompts). It skips steps that are already done --
if the PUDL database is already downloaded, it won't re-download it. The full run
takes 15-30 minutes depending on download speed and hardware.

**Options** (set as environment variables):

| Variable | Description |
|---|---|
| `PUDL_DB` | Path to an existing PUDL `.sqlite` file (skips download) |
| `SKIP_BUILD` | Set to `1` to skip the Maven build |
| `SKIP_BENCHMARK` | Set to `1` to skip the SQLite benchmark, run only the cross-connector test |
| `BENCHMARK_RUNS` | Number of recorded runs per query (default: `3`) |

**What it runs:**

1. `demo/benchmark_pudl.py --large` -- benchmarks COUNT(\*), aggregation, joins,
   and pagination on tables from 19K to 3.3M rows
2. `demo/cross_connector_test.py` -- 12 federated queries joining SQLite energy
   data with PostgreSQL reference tables (state demographics, emission factors,
   regulatory inspections), including 4-way cross-catalog joins

For detailed results and analysis, see [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md).

---

## Project layout

```
presto-sqlite/
  pom.xml
  install.sh                         Setup script (Linux / macOS)
  install.ps1                        Setup script (Windows)
  run_pudl_benchmark.sh              PUDL benchmark runner (Linux / macOS)
  run_pudl_benchmark.ps1             PUDL benchmark runner (Windows)
  BENCHMARK_REPORT.md                Performance analysis and results
  src/
    assembly/plugin.xml              Assembly descriptor for plugin packaging
    main/java/.../sqlite/
      SqlitePlugin.java              SPI entry point
      SqliteConnectorFactory.java    Creates connectors from catalog config
      SqliteMetadata.java            Schema / table / column metadata + predicate pushdown
      SqliteSplitManager.java        Multi-split parallelism via ROWID ranges
      SqliteRecordSetProvider.java   Bridges splits to record sets
      SqliteRecordSet.java           RecordSet + RecordCursor (JDBC reads + COUNT pushdown)
      SqliteClient.java              HikariCP connection pool
      SqliteColumnHandle.java        Column handle (name, type, ordinal)
      SqliteTableHandle.java         Table handle (schema, table name)
      SqliteTableLayoutHandle.java   Layout wrapper (carries WHERE clause)
      SqliteSplit.java               Split definition (WHERE + ROWID range)
      SqliteHandleResolver.java      Handle class resolution
      SqliteTransactionHandle.java   Transaction handle (singleton)
    main/resources/META-INF/services/
      com.facebook.presto.spi.Plugin
  demo/
    create_sqlite_db.py              Creates the sample SQLite database
    create_postgres_db.py            Seeds the sample PostgreSQL database
    create_postgres_pudl.py          Seeds PostgreSQL with PUDL reference data
    query_presto.py                  Demo queries (SQLite, PostgreSQL, cross-catalog)
    benchmark_pudl.py                PUDL benchmark suite (COUNT, aggregation, joins)
    cross_connector_test.py          Cross-connector federation test (SQLite + PostgreSQL)
    query_all.sql                    Same queries for the Presto CLI
```

---

## License

Apache License 2.0
