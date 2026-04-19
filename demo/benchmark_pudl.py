"""

Benchmark suite for the Presto SQLite connector against the PUDL database.


Tests raw throughput, aggregation, complex joins, and pagination latency

across multiple runs to produce stable, statistically meaningful results.


Usage:

    python benchmark_pudl.py              # default: medium tables (~250K-767K rows)

    python benchmark_pudl.py --small      # small tables only (~19K rows) — fast sanity check

    python benchmark_pudl.py --large      # include the 3.3M row table — slow but thorough

    python benchmark_pudl.py --timeout 60 # per-query timeout in seconds (default: 120)

    python benchmark_pudl.py --runs 3     # number of recorded runs (default: 5)

"""


import argparse

import statistics

import sys

import time

import threading


import prestodb

from requests.exceptions import ConnectionError


PRESTO_HOST = "localhost"

PRESTO_PORT = 8080

PRESTO_USER = "test"

CATALOG = "sqlite"

SCHEMA = "default"


# ─── Table aliases for readability ──────────────────────────────────────────

# Small  (~19K rows):  core_eia__entity_plants

# Medium (~252K rows): core_eia860__scd_plants

# Medium (~665K rows): core_eia860__scd_generators

# Medium (~767K rows): core_eia923__monthly_generation

# Large  (~3.3M rows): core_eia923__monthly_generation_fuel


QUERIES_SMALL = [

    {

        "name": "COUNT(*) Small",

        "description": "COUNT(*) on entity_plants (~19K rows)",

        "sql": f"""

            SELECT COUNT(*) AS total_rows

            FROM {CATALOG}."{SCHEMA}".core_eia__entity_plants

        """,

    },

    {

        "name": "Aggregation Small",

        "description": "Plants per state with COUNT/MIN/MAX on entity_plants",

        "sql": f"""

            SELECT

                state,

                COUNT(*)       AS plant_count,

                MIN(latitude)  AS min_lat,

                MAX(latitude)  AS max_lat

            FROM {CATALOG}."{SCHEMA}".core_eia__entity_plants

            GROUP BY state

            ORDER BY plant_count DESC

        """,

    },

    {

        "name": "LIMIT 1000 Small",

        "description": "Fetch first 1000 rows from entity_plants",

        "sql": f"""

            SELECT *

            FROM {CATALOG}."{SCHEMA}".core_eia__entity_plants

            LIMIT 1000

        """,

    },

]


QUERIES_MEDIUM = [

    {

        "name": "COUNT(*) Medium",

        "description": "COUNT(*) on monthly_generation (~767K rows)",

        "sql": f"""

            SELECT COUNT(*) AS total_rows

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation

        """,

    },

    {

        "name": "Aggregation Medium",

        "description": "Yearly generation SUM/AVG grouped by plant on monthly_generation",

        "sql": f"""

            SELECT

                plant_id_eia,

                SUBSTR(CAST(report_date AS VARCHAR), 1, 4) AS report_year,

                COUNT(*)                    AS records,

                SUM(net_generation_mwh)     AS total_mwh,

                AVG(net_generation_mwh)     AS avg_mwh

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation

            GROUP BY plant_id_eia,

                     SUBSTR(CAST(report_date AS VARCHAR), 1, 4)

            ORDER BY total_mwh DESC

            LIMIT 100

        """,

    },

    {

        "name": "Join (767K x 19K)",

        "description": "Join monthly_generation with entity_plants on plant_id_eia",

        "sql": f"""

            SELECT

                p.state,

                p.plant_name_eia,

                COUNT(*)                    AS months_reported,

                SUM(g.net_generation_mwh)   AS total_mwh,

                AVG(g.net_generation_mwh)   AS avg_monthly_mwh

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation g

            JOIN {CATALOG}."{SCHEMA}".core_eia__entity_plants p

                ON g.plant_id_eia = p.plant_id_eia

            GROUP BY p.state, p.plant_name_eia

            HAVING SUM(g.net_generation_mwh) > 0

            ORDER BY total_mwh DESC

            LIMIT 50

        """,

    },

    {

        "name": "LIMIT 1000 Medium",

        "description": "First 1000 rows from monthly_generation — first-batch latency",

        "sql": f"""

            SELECT *

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation

            LIMIT 1000

        """,

    },

]


QUERIES_LARGE = [

    {

        "name": "COUNT(*) Large",

        "description": "COUNT(*) on generation_fuel (~3.3M rows) — full table scan",

        "sql": f"""

            SELECT COUNT(*) AS total_rows

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation_fuel

        """,

    },

    {

        "name": "Aggregation Large",

        "description": "Yearly totals by fuel type on generation_fuel (~3.3M rows)",

        "sql": f"""

            SELECT

                fuel_type_code_pudl,

                SUBSTR(CAST(report_date AS VARCHAR), 1, 4) AS report_year,

                COUNT(*)                                    AS record_count,

                SUM(net_generation_mwh)                     AS total_generation_mwh,

                SUM(fuel_consumed_mmbtu)                    AS total_fuel_mmbtu

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation_fuel

            GROUP BY fuel_type_code_pudl,

                     SUBSTR(CAST(report_date AS VARCHAR), 1, 4)

            ORDER BY report_year DESC, total_generation_mwh DESC

        """,

    },

    {

        "name": "Join Large (3.3M x 19K)",

        "description": "Join generation_fuel with entity_plants — filter coal only",

        "sql": f"""

            SELECT

                p.state,

                SUM(gf.net_generation_mwh)      AS coal_generation_mwh,

                SUM(gf.fuel_consumed_mmbtu)     AS coal_fuel_mmbtu,

                COUNT(DISTINCT gf.plant_id_eia) AS plant_count

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation_fuel gf

            JOIN {CATALOG}."{SCHEMA}".core_eia__entity_plants p

                ON gf.plant_id_eia = p.plant_id_eia

            WHERE gf.fuel_type_code_pudl = 'coal'

            GROUP BY p.state

            ORDER BY coal_generation_mwh DESC

        """,

    },

    {

        "name": "LIMIT 1000 Large",

        "description": "First 1000 rows from generation_fuel (~3.3M rows)",

        "sql": f"""

            SELECT *

            FROM {CATALOG}."{SCHEMA}".core_eia923__monthly_generation_fuel

            LIMIT 1000

        """,

    },

]




def connect():

    return prestodb.dbapi.connect(

        host=PRESTO_HOST,

        port=PRESTO_PORT,

        user=PRESTO_USER,

    )




def run_timed_query(cursor, sql, timeout_sec):

    timed_out = threading.Event()


    def timeout_handler():

        timed_out.set()

        try:

            cursor.cancel()

        except Exception:

            pass


    timer = threading.Timer(timeout_sec, timeout_handler)

    timer.start()

    try:

        start = time.perf_counter()

        cursor.execute(sql)

        rows = cursor.fetchall()

        elapsed = time.perf_counter() - start

        if timed_out.is_set():

            return None, 0

        return elapsed, len(rows)

    except Exception:

        if timed_out.is_set():

            return None, 0

        raise

    finally:

        timer.cancel()




def benchmark_query(conn, query_info, warmup_runs, recorded_runs, timeout_sec):

    name = query_info["name"]

    sql = query_info["sql"]


    print(f"\n  Benchmarking: {name}")

    print(f"  {query_info['description']}")


    for i in range(warmup_runs):

        cur = conn.cursor()

        elapsed, row_count = run_timed_query(cur, sql, timeout_sec)

        if elapsed is None:

            print(f"    Warmup {i + 1}/{warmup_runs}: TIMEOUT (>{timeout_sec}s) — skipping query")

            return None

        print(f"    Warmup {i + 1}/{warmup_runs}: {elapsed:.3f}s ({row_count:,} rows)")


    timings = []

    final_row_count = 0

    for i in range(recorded_runs):

        cur = conn.cursor()

        elapsed, row_count = run_timed_query(cur, sql, timeout_sec)

        if elapsed is None:

            print(f"    Run {i + 1}/{recorded_runs}: TIMEOUT (>{timeout_sec}s) — skipping query")

            return None

        timings.append(elapsed)

        final_row_count = row_count

        print(f"    Run {i + 1}/{recorded_runs}: {elapsed:.3f}s")


    return {

        "name": name,

        "row_count": final_row_count,

        "timings": timings,

        "avg": statistics.mean(timings),

        "median": statistics.median(timings),

        "min": min(timings),

        "max": max(timings),

        "stdev": statistics.stdev(timings) if len(timings) > 1 else 0.0,

    }




def print_results(results, warmup_runs, recorded_runs):

    print("\n")

    print("=" * 105)

    print(f"{'BENCHMARK RESULTS':^105}")

    print(f"{'(' + str(warmup_runs) + ' warmup + ' + str(recorded_runs) + ' recorded runs per query)':^105}")

    print("=" * 105)


    header = (

        f"{'Query':<30} {'Rows':>10} {'Avg (s)':>10} {'Median':>10} "

        f"{'Min (s)':>10} {'Max (s)':>10} {'StdDev':>10}"

    )

    print(header)

    print("-" * 105)


    for r in results:

        row = (

            f"{r['name']:<30} {r['row_count']:>10,} {r['avg']:>10.3f} {r['median']:>10.3f} "

            f"{r['min']:>10.3f} {r['max']:>10.3f} {r['stdev']:>10.3f}"

        )

        print(row)


    print("-" * 105)


    total_avg = sum(r["avg"] for r in results)

    total_queries = len(results) * recorded_runs + len(results) * warmup_runs

    print(f"\n  Total average time (all queries): {total_avg:.3f}s")

    print(f"  Total queries executed:           {total_queries}")

    print(f"  Fastest query:                    {min(results, key=lambda r: r['min'])['name']} ({min(r['min'] for r in results):.3f}s)")

    print(f"  Slowest query:                    {max(results, key=lambda r: r['max'])['name']} ({max(r['max'] for r in results):.3f}s)")

    print()




def main():

    parser = argparse.ArgumentParser(description="Presto SQLite Connector — PUDL Benchmark Suite")

    parser.add_argument("--small", action="store_true", help="Run only small table queries (~19K rows)")

    parser.add_argument("--large", action="store_true", help="Include large table queries (~3.3M rows)")

    parser.add_argument("--timeout", type=int, default=120, help="Per-query timeout in seconds (default: 120)")

    parser.add_argument("--runs", type=int, default=5, help="Number of recorded runs per query (default: 5)")

    parser.add_argument("--warmup", type=int, default=1, help="Number of warmup runs per query (default: 1)")

    args = parser.parse_args()


    if args.small:

        queries = QUERIES_SMALL

        tier = "SMALL (~19K rows)"

    elif args.large:

        queries = QUERIES_SMALL + QUERIES_MEDIUM + QUERIES_LARGE

        tier = "ALL (19K → 3.3M rows)"

    else:

        queries = QUERIES_SMALL + QUERIES_MEDIUM

        tier = "SMALL + MEDIUM (~19K → 767K rows)"


    print("Presto SQLite Connector — PUDL Benchmark Suite")

    print(f"Target:  {PRESTO_HOST}:{PRESTO_PORT}, catalog={CATALOG}")

    print(f"Tier:    {tier}")

    print(f"Config:  {args.warmup} warmup + {args.runs} recorded runs, {args.timeout}s timeout")

    print()


    try:

        conn = connect()

        cur = conn.cursor()

        cur.execute("SELECT 1")

        cur.fetchall()

        cur.close()

        print("Connected to Presto successfully.")

    except ConnectionError:

        print(f"ERROR: Could not connect to Presto at {PRESTO_HOST}:{PRESTO_PORT}.")

        print("Make sure the Presto server is running:")

        print("  docker start presto-postgres presto")

        sys.exit(1)


    results = []

    skipped = []

    for query_info in queries:

        try:

            result = benchmark_query(conn, query_info, args.warmup, args.runs, args.timeout)

            if result:

                results.append(result)

            else:

                skipped.append(query_info["name"])

        except Exception as e:

            print(f"\n  FAILED: {query_info['name']}")

            print(f"    Error: {e}")

            skipped.append(query_info["name"])


    conn.close()


    if results:

        print_results(results, args.warmup, args.runs)


    if skipped:

        print(f"  Skipped/failed ({len(skipped)}):")

        for name in skipped:

            print(f"    - {name}")

        print()


    if not results:

        print("\nNo queries completed successfully.")

        sys.exit(1)




if __name__ == "__main__":

    main()