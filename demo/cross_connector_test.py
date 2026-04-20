"""Cross-connector test: queries both SQLite (PUDL) and PostgreSQL through Presto.


Demonstrates Presto's federated query capability by joining energy generation

data from SQLite with reference/regulatory data from PostgreSQL in a single query.


Prerequisites:

    docker start presto-postgres presto

    python demo/create_postgres_pudl.py   # seeds PostgreSQL reference tables


Usage:

    python demo/cross_connector_test.py           # run all tests

    python demo/cross_connector_test.py --quick   # skip large-table queries

"""


import argparse

import sys

import time


import prestodb

from prestodb.exceptions import PrestoUserError

from requests.exceptions import ConnectionError


PRESTO_HOST = "localhost"

PRESTO_PORT = 8080

PRESTO_USER = "test"


SQLITE_CATALOG = "sqlite"

SQLITE_SCHEMA = "default"

PG_CATALOG = "postgres"

PG_SCHEMA = "public"


SQ = f'{SQLITE_CATALOG}."{SQLITE_SCHEMA}"'

PG = f"{PG_CATALOG}.{PG_SCHEMA}"




def connect():

    return prestodb.dbapi.connect(host=PRESTO_HOST, port=PRESTO_PORT, user=PRESTO_USER)




def run_query(cursor, sql, title):

    print(f"\n{'=' * 90}")

    print(f"  {title}")

    print(f"{'=' * 90}")

    print(f"  SQL: {' '.join(sql.split())[:120]}...")

    print()


    start = time.perf_counter()

    cursor.execute(sql)

    rows = cursor.fetchall()

    elapsed = time.perf_counter() - start


    if not cursor.description:

        print(f"  (no results, {elapsed:.3f}s)")

        return rows, elapsed


    col_names = [desc[0] for desc in cursor.description]

    widths = [len(name) for name in col_names]

    display_rows = rows[:25]

    for row in display_rows:

        for i, val in enumerate(row):

            widths[i] = max(widths[i], min(len(str(val)), 40))


    header = " | ".join(name.ljust(widths[i]) for i, name in enumerate(col_names))

    sep = "-+-".join("-" * w for w in widths)

    print(f"  {header}")

    print(f"  {sep}")

    for row in display_rows:

        line = " | ".join(str(val)[:40].ljust(widths[i]) for i, val in enumerate(row))

        print(f"  {line}")


    if len(rows) > 25:

        print(f"  ... ({len(rows) - 25} more rows)")


    print(f"\n  ({len(rows):,} rows, {elapsed:.3f}s)")

    return rows, elapsed




def main():

    parser = argparse.ArgumentParser(description="Cross-connector test: SQLite + PostgreSQL via Presto")

    parser.add_argument("--quick", action="store_true", help="Skip large-table queries (3.3M rows)")

    args = parser.parse_args()


    print("Cross-Connector Federation Test")

    print(f"  Presto:     {PRESTO_HOST}:{PRESTO_PORT}")

    print(f"  SQLite:     {SQ} (PUDL energy database)")

    print(f"  PostgreSQL: {PG} (reference/regulatory data)")

    print()


    # -- Connectivity --


    try:

        conn = connect()

        cur = conn.cursor()

        cur.execute("SELECT 1")

        cur.fetchall()

    except ConnectionError:

        print(f"ERROR: Cannot connect to Presto at {PRESTO_HOST}:{PRESTO_PORT}")

        print("  docker start presto-postgres presto")

        sys.exit(1)


    results = []


    # =========================================================================

    # Section 1: Single-catalog sanity checks

    # =========================================================================


    print("\n" + "#" * 90)

    print("  SECTION 1: Single-Catalog Queries (sanity checks)")

    print("#" * 90)


    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT state_code, state_name, region, population_2023, renewable_target_pct

        FROM {PG}.state_energy_profiles

        WHERE renewable_target_pct IS NOT NULL

        ORDER BY renewable_target_pct DESC

        LIMIT 10

    """, "[PG] Top 10 states by renewable energy target")

    results.append(("PG: Top renewable targets", t))


    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT fuel_type_code, fuel_name, co2_kg_per_mwh, renewable, category

        FROM {PG}.fuel_emission_factors

        ORDER BY co2_kg_per_mwh DESC

    """, "[PG] Fuel emission factors")

    results.append(("PG: Emission factors", t))


    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT state, COUNT(*) AS plant_count

        FROM {SQ}.core_eia__entity_plants

        GROUP BY state

        ORDER BY plant_count DESC

        LIMIT 10

    """, "[SQLite] Top 10 states by plant count")

    results.append(("SQLite: Plants by state", t))


    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT fuel_type_code_pudl, COUNT(*) AS records, SUM(net_generation_mwh) AS total_mwh

        FROM {SQ}.core_eia923__monthly_generation_fuel

        GROUP BY fuel_type_code_pudl

        ORDER BY total_mwh DESC

    """, "[SQLite] Generation by fuel type (all time)")

    results.append(("SQLite: Gen by fuel type", t))


    # =========================================================================

    # Section 2: Cross-catalog joins

    # =========================================================================


    print("\n" + "#" * 90)

    print("  SECTION 2: Cross-Catalog Joins (SQLite + PostgreSQL)")

    print("#" * 90)


    # Join 1: Plants + State profiles

    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT

            sp.region,

            sp.state_name,

            sp.population_2023,

            COUNT(p.plant_id_eia) AS plant_count,

            ROUND(CAST(COUNT(p.plant_id_eia) AS DOUBLE) / sp.population_2023 * 1000000, 1) AS plants_per_million

        FROM {PG}.state_energy_profiles sp

        JOIN {SQ}.core_eia__entity_plants p

            ON sp.state_code = p.state

        GROUP BY sp.region, sp.state_name, sp.population_2023

        ORDER BY plants_per_million DESC

        LIMIT 15

    """, "[CROSS] Power plant density by state (SQLite plants + PG demographics)")

    results.append(("CROSS: Plant density", t))


    # Join 2: Fuel generation + Emission factors -> CO2 estimates

    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT

            ef.fuel_name,

            ef.category,

            ef.co2_kg_per_mwh,

            COUNT(*) AS records,

            ROUND(SUM(gf.net_generation_mwh), 0) AS total_mwh,

            ROUND(SUM(gf.net_generation_mwh) * ef.co2_kg_per_mwh / 1e9, 2) AS est_co2_million_tonnes

        FROM {SQ}.core_eia923__monthly_generation_fuel gf

        JOIN {PG}.fuel_emission_factors ef

            ON gf.fuel_type_code_pudl = ef.fuel_type_code

        GROUP BY ef.fuel_name, ef.category, ef.co2_kg_per_mwh

        ORDER BY est_co2_million_tonnes DESC

    """, "[CROSS] Estimated CO2 by fuel type (SQLite generation + PG emission factors)")

    results.append(("CROSS: CO2 by fuel", t))


    # Join 3: Plants + State profiles + Generation -> Regional renewable analysis

    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT

            sp.region,

            COUNT(DISTINCT p.plant_id_eia) AS total_plants,

            COUNT(DISTINCT p.state) AS states,

            ROUND(AVG(sp.renewable_target_pct), 1) AS avg_renewable_target,

            SUM(sp.population_2023) / COUNT(DISTINCT p.plant_id_eia) AS pop_per_plant

        FROM {PG}.state_energy_profiles sp

        JOIN {SQ}.core_eia__entity_plants p

            ON sp.state_code = p.state

        WHERE sp.renewable_target_pct IS NOT NULL

        GROUP BY sp.region

        ORDER BY avg_renewable_target DESC

    """, "[CROSS] Regional renewable targets vs plant infrastructure")

    results.append(("CROSS: Regional renewables", t))


    # Join 4: Plant inspections + Plant locations (PG inspections + SQLite plant info)

    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT

            p.plant_name_eia,

            p.state,

            p.city,

            ins.inspection_date,

            ins.result,

            ins.violations,

            ins.inspector

        FROM {PG}.plant_inspections ins

        JOIN {SQ}.core_eia__entity_plants p

            ON ins.plant_id_eia = p.plant_id_eia

        ORDER BY ins.inspection_date DESC

    """, "[CROSS] Plant inspection history with locations (PG inspections + SQLite plants)")

    results.append(("CROSS: Inspections+plants", t))


    # Join 5: Failed inspections with state regulatory context

    cur = conn.cursor()

    _, t = run_query(cur, f"""

        SELECT

            p.plant_name_eia,

            p.state,

            sp.region,

            sp.renewable_target_pct,

            sp.dereg_market,

            ins.result,

            ins.violations,

            ins.notes

        FROM {PG}.plant_inspections ins

        JOIN {SQ}.core_eia__entity_plants p

            ON ins.plant_id_eia = p.plant_id_eia

        JOIN {PG}.state_energy_profiles sp

            ON p.state = sp.state_code

        WHERE ins.result != 'pass'

        ORDER BY ins.violations DESC

    """, "[CROSS] Non-passing inspections with regulatory context (3-way: PG+SQLite+PG)")

    results.append(("CROSS: Failed inspections", t))


    # =========================================================================

    # Section 3: Heavy cross-catalog analytics (skipped with --quick)

    # =========================================================================


    if not args.quick:

        print("\n" + "#" * 90)

        print("  SECTION 3: Heavy Cross-Catalog Analytics (large tables)")

        print("#" * 90)


        # Join 6: State-level CO2 estimate (3.3M gen_fuel + plants + states + emission factors)

        cur = conn.cursor()

        _, t = run_query(cur, f"""

            SELECT

                sp.state_name,

                sp.region,

                sp.renewable_target_pct,

                COUNT(*) AS fuel_records,

                ROUND(SUM(gf.net_generation_mwh), 0) AS total_mwh,

                ROUND(SUM(gf.net_generation_mwh * ef.co2_kg_per_mwh) / 1e9, 2) AS est_co2_mt

            FROM {SQ}.core_eia923__monthly_generation_fuel gf

            JOIN {SQ}.core_eia__entity_plants p

                ON gf.plant_id_eia = p.plant_id_eia

            JOIN {PG}.state_energy_profiles sp

                ON p.state = sp.state_code

            JOIN {PG}.fuel_emission_factors ef

                ON gf.fuel_type_code_pudl = ef.fuel_type_code

            GROUP BY sp.state_name, sp.region, sp.renewable_target_pct

            ORDER BY est_co2_mt DESC

            LIMIT 20

        """, "[CROSS] Top CO2-emitting states (4-way join: SQLite gen_fuel + plants + PG states + emissions)")

        results.append(("CROSS: State CO2 (4-way)", t))


        # Join 7: Renewable vs fossil generation by region

        cur = conn.cursor()

        _, t = run_query(cur, f"""

            SELECT

                sp.region,

                ef.category AS fuel_category,

                COUNT(DISTINCT gf.plant_id_eia) AS plants,

                ROUND(SUM(gf.net_generation_mwh), 0) AS total_mwh,

                ROUND(SUM(gf.net_generation_mwh * ef.co2_kg_per_mwh) / 1e6, 0) AS co2_tonnes

            FROM {SQ}.core_eia923__monthly_generation_fuel gf

            JOIN {SQ}.core_eia__entity_plants p

                ON gf.plant_id_eia = p.plant_id_eia

            JOIN {PG}.state_energy_profiles sp

                ON p.state = sp.state_code

            JOIN {PG}.fuel_emission_factors ef

                ON gf.fuel_type_code_pudl = ef.fuel_type_code

            GROUP BY sp.region, ef.category

            ORDER BY sp.region, total_mwh DESC

        """, "[CROSS] Renewable vs fossil by region (4-way join)")

        results.append(("CROSS: Renew vs fossil", t))


        # Join 8: Deregulated vs regulated market comparison

        cur = conn.cursor()

        _, t = run_query(cur, f"""

            SELECT

                CASE WHEN sp.dereg_market THEN 'Deregulated' ELSE 'Regulated' END AS market_type,

                COUNT(DISTINCT p.state) AS states,

                COUNT(DISTINCT gf.plant_id_eia) AS plants,

                ROUND(SUM(gf.net_generation_mwh) / 1e6, 1) AS total_twh,

                ROUND(SUM(gf.net_generation_mwh * ef.co2_kg_per_mwh) / 1e12, 3) AS co2_billion_tonnes,

                ROUND(

                    SUM(CASE WHEN ef.renewable THEN gf.net_generation_mwh ELSE 0 END)

                    / NULLIF(SUM(gf.net_generation_mwh), 0) * 100, 1

                ) AS renewable_pct

            FROM {SQ}.core_eia923__monthly_generation_fuel gf

            JOIN {SQ}.core_eia__entity_plants p

                ON gf.plant_id_eia = p.plant_id_eia

            JOIN {PG}.state_energy_profiles sp

                ON p.state = sp.state_code

            JOIN {PG}.fuel_emission_factors ef

                ON gf.fuel_type_code_pudl = ef.fuel_type_code

            GROUP BY sp.dereg_market

            ORDER BY market_type

        """, "[CROSS] Deregulated vs regulated markets (energy mix comparison)")

        results.append(("CROSS: Dereg vs reg", t))

    else:

        print("\n  (Skipping large-table queries -- use without --quick to include them)")


    # =========================================================================

    # Summary

    # =========================================================================


    print("\n" + "=" * 90)

    print(f"  {'TIMING SUMMARY':^86}")

    print("=" * 90)

    print(f"  {'Query':<45} {'Time (s)':>10}")

    print(f"  {'-' * 45} {'-' * 10}")


    total = 0.0

    for name, t in results:

        print(f"  {name:<45} {t:>10.3f}")

        total += t


    print(f"  {'-' * 45} {'-' * 10}")

    print(f"  {'TOTAL':<45} {total:>10.3f}")

    print()


    pg_time = sum(t for n, t in results if n.startswith("PG:"))

    sq_time = sum(t for n, t in results if n.startswith("SQLite:"))

    cx_time = sum(t for n, t in results if n.startswith("CROSS:"))


    print(f"  PostgreSQL-only queries:  {pg_time:.3f}s")

    print(f"  SQLite-only queries:      {sq_time:.3f}s")

    print(f"  Cross-catalog queries:    {cx_time:.3f}s")

    print()


    conn.close()

    print("All tests passed. Presto successfully federated queries across SQLite and PostgreSQL.")




if __name__ == "__main__":

    main()