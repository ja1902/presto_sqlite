"""Seeds the PostgreSQL database with energy-sector reference data for cross-connector

queries against the PUDL SQLite database.


Tables created:

    state_energy_profiles  - State-level demographics and renewable energy targets

    fuel_emission_factors  - CO2 emission factors per fuel type (joins on fuel_type_code_pudl)

    plant_inspections      - Regulatory inspection records (joins on plant_id_eia)


Usage:

    docker start presto-postgres

    python demo/create_postgres_pudl.py

"""

import time

import psycopg2


PG_HOST = "localhost"

PG_PORT = 5433

PG_DB = "demo"

PG_USER = "presto"

PG_PASS = "presto"




def connect(retries=20):

    for attempt in range(retries):

        try:

            return psycopg2.connect(

                host=PG_HOST, port=PG_PORT, dbname=PG_DB,

                user=PG_USER, password=PG_PASS,

            )

        except psycopg2.OperationalError:

            if attempt == retries - 1:

                raise

            time.sleep(2)




def main():

    print("Connecting to PostgreSQL...")

    conn = connect()

    conn.autocommit = False

    cur = conn.cursor()


    # -- state_energy_profiles --

    cur.execute("DROP TABLE IF EXISTS plant_inspections")

    cur.execute("DROP TABLE IF EXISTS fuel_emission_factors")

    cur.execute("DROP TABLE IF EXISTS state_energy_profiles")


    cur.execute("""

        CREATE TABLE state_energy_profiles (

            state_code          TEXT PRIMARY KEY,

            state_name          TEXT NOT NULL,

            region              TEXT NOT NULL,

            population_2023     BIGINT NOT NULL,

            renewable_target_pct NUMERIC(5,2),

            renewable_target_year INTEGER,

            dereg_market        BOOLEAN NOT NULL DEFAULT FALSE

        )

    """)


    states = [

        ("AL", "Alabama",        "Southeast",  5108468,  None,  None, False),

        ("AK", "Alaska",         "West",        733536,  None,  None, False),

        ("AZ", "Arizona",        "Southwest",  7303398, 15.00, 2025, True),

        ("AR", "Arkansas",       "Southeast",  3067732,  None,  None, False),

        ("CA", "California",     "West",       38965193, 60.00, 2030, True),

        ("CO", "Colorado",       "West",        5877610, 30.00, 2030, False),

        ("CT", "Connecticut",    "Northeast",   3617176, 48.00, 2030, True),

        ("DE", "Delaware",       "Northeast",   1031890, 40.00, 2035, True),

        ("FL", "Florida",        "Southeast",  22610726,  None,  None, False),

        ("GA", "Georgia",        "Southeast",  11029227,  None,  None, False),

        ("HI", "Hawaii",         "West",        1435138, 100.0, 2045, False),

        ("ID", "Idaho",          "West",        1964726,  None,  None, False),

        ("IL", "Illinois",       "Midwest",    12549689, 40.00, 2030, True),

        ("IN", "Indiana",        "Midwest",     6862199, 10.00, 2025, False),

        ("IA", "Iowa",           "Midwest",     3207004,  None,  None, False),

        ("KS", "Kansas",         "Midwest",     2940546, 20.00, 2020, False),

        ("KY", "Kentucky",       "Southeast",   4526154,  None,  None, False),

        ("LA", "Louisiana",      "Southeast",   4573749,  None,  None, False),

        ("ME", "Maine",          "Northeast",   1395722, 80.00, 2030, True),

        ("MD", "Maryland",       "Northeast",   6180253, 50.00, 2030, True),

        ("MA", "Massachusetts",  "Northeast",   7001399, 40.00, 2030, True),

        ("MI", "Michigan",       "Midwest",    10037261, 15.00, 2025, True),

        ("MN", "Minnesota",      "Midwest",     5737915, 26.50, 2025, False),

        ("MS", "Mississippi",    "Southeast",   2939690,  None,  None, False),

        ("MO", "Missouri",       "Midwest",     6196156, 15.00, 2021, False),

        ("MT", "Montana",        "West",        1132812, 15.00, 2015, True),

        ("NE", "Nebraska",       "Midwest",     1978379,  None,  None, False),

        ("NV", "Nevada",         "West",        3194176, 50.00, 2030, True),

        ("NH", "New Hampshire",  "Northeast",   1402054, 25.20, 2025, True),

        ("NJ", "New Jersey",     "Northeast",   9290841, 50.00, 2030, True),

        ("NM", "New Mexico",     "Southwest",   2114371, 50.00, 2030, False),

        ("NY", "New York",       "Northeast",  19571216, 70.00, 2030, True),

        ("NC", "North Carolina", "Southeast",  10835491, 12.50, 2021, False),

        ("ND", "North Dakota",   "Midwest",      783926, 10.00, 2015, False),

        ("OH", "Ohio",           "Midwest",    11785935, 12.50, 2027, True),

        ("OK", "Oklahoma",       "Southwest",   4053824, 15.00, 2015, False),

        ("OR", "Oregon",         "West",        4233358, 50.00, 2040, False),

        ("PA", "Pennsylvania",   "Northeast",  12961683, 18.00, 2021, True),

        ("RI", "Rhode Island",   "Northeast",   1095962, 38.50, 2035, True),

        ("SC", "South Carolina", "Southeast",   5373555,  None,  None, False),

        ("SD", "South Dakota",   "Midwest",      919318, 10.00, 2015, False),

        ("TN", "Tennessee",      "Southeast",   7126489,  None,  None, False),

        ("TX", "Texas",          "Southwest",  30503301, 10.00, 2025, True),

        ("UT", "Utah",           "West",        3417734, 20.00, 2025, False),

        ("VT", "Vermont",        "Northeast",    647464, 75.00, 2032, False),

        ("VA", "Virginia",       "Southeast",   8683619, 30.00, 2030, True),

        ("WA", "Washington",     "West",        7812880, 15.00, 2020, False),

        ("WV", "West Virginia",  "Southeast",   1770071,  None,  None, False),

        ("WI", "Wisconsin",      "Midwest",     5910955, 10.00, 2015, False),

        ("WY", "Wyoming",        "West",         584057,  None,  None, False),

        ("DC", "Dist. Columbia", "Northeast",    678972, 100.0, 2032, True),

        ("PR", "Puerto Rico",    "Caribbean",   3205691, 40.00, 2025, False),

    ]


    cur.executemany(

        "INSERT INTO state_energy_profiles VALUES (%s,%s,%s,%s,%s,%s,%s)",

        states,

    )

    print(f"  state_energy_profiles: {len(states)} rows")


    # -- fuel_emission_factors --

    cur.execute("""

        CREATE TABLE fuel_emission_factors (

            fuel_type_code      TEXT PRIMARY KEY,

            fuel_name           TEXT NOT NULL,

            co2_kg_per_mwh      NUMERIC(10,2) NOT NULL,

            nox_g_per_mwh       NUMERIC(10,2) NOT NULL,

            so2_g_per_mwh       NUMERIC(10,2) NOT NULL,

            renewable           BOOLEAN NOT NULL DEFAULT FALSE,

            category            TEXT NOT NULL

        )

    """)


    fuels = [

        ("coal",  "Coal",           980.0,  1800.0, 5200.0, False, "fossil"),

        ("gas",   "Natural Gas",    410.0,   640.0,   10.0, False, "fossil"),

        ("oil",   "Petroleum",      840.0,  1400.0, 4600.0, False, "fossil"),

        ("hydro", "Hydroelectric",    4.0,     0.0,    0.0, True,  "renewable"),

        ("solar", "Solar",            5.0,     0.0,    0.0, True,  "renewable"),

        ("wind",  "Wind",             4.0,     0.0,    0.0, True,  "renewable"),

        ("waste", "Waste/Biomass",  100.0,   500.0,  200.0, False, "other"),

        ("other", "Other",          200.0,   300.0,  100.0, False, "other"),

    ]


    cur.executemany(

        "INSERT INTO fuel_emission_factors VALUES (%s,%s,%s,%s,%s,%s,%s)",

        fuels,

    )

    print(f"  fuel_emission_factors: {len(fuels)} rows")


    # -- plant_inspections --

    cur.execute("""

        CREATE TABLE plant_inspections (

            id              SERIAL PRIMARY KEY,

            plant_id_eia    INTEGER NOT NULL,

            inspection_date DATE NOT NULL,

            inspector       TEXT NOT NULL,

            result          TEXT NOT NULL,

            violations      INTEGER NOT NULL DEFAULT 0,

            notes           TEXT

        )

    """)


    # Pick a handful of real plant IDs from the PUDL data and assign inspection records.

    inspections = [

        (3, '2023-06-15', 'J. Martinez', 'pass', 0, None),

        (3, '2024-03-10', 'J. Martinez', 'pass', 0, None),

        (6, '2023-08-22', 'A. Chen', 'fail', 2, 'Exceeded NOx limits; cooling water discharge temp high'),

        (6, '2024-02-14', 'A. Chen', 'pass', 0, 'Follow-up: issues resolved'),

        (10, '2023-05-01', 'R. Patel', 'pass', 0, None),

        (10, '2024-01-20', 'R. Patel', 'conditional', 1, 'Minor ash disposal documentation gap'),

        (26, '2023-07-18', 'S. Kim', 'pass', 0, None),

        (47, '2023-09-05', 'L. Johnson', 'fail', 3, 'Stack emissions over limit; missing calibration records; safety signage'),

        (47, '2024-04-01', 'L. Johnson', 'pass', 0, 'Full remediation verified'),

        (50, '2023-11-30', 'M. Davis', 'pass', 0, None),

        (50, '2024-06-15', 'M. Davis', 'pass', 0, None),

        (54, '2023-04-12', 'J. Martinez', 'conditional', 1, 'Pending wastewater permit renewal'),

        (62, '2024-01-08', 'A. Chen', 'pass', 0, None),

        (100, '2023-10-15', 'R. Patel', 'pass', 0, None),

        (100, '2024-05-20', 'R. Patel', 'pass', 0, None),

        (113, '2023-12-01', 'S. Kim', 'fail', 1, 'Particulate emissions exceeded seasonal threshold'),

        (113, '2024-07-10', 'S. Kim', 'pass', 0, 'New scrubber installed'),

        (151, '2024-02-28', 'L. Johnson', 'pass', 0, None),

        (200, '2023-06-20', 'M. Davis', 'conditional', 2, 'Noise ordinance; stormwater runoff'),

        (200, '2024-03-15', 'M. Davis', 'pass', 0, 'Both resolved'),

        (500, '2023-08-01', 'J. Martinez', 'pass', 0, None),

        (880, '2023-09-14', 'A. Chen', 'pass', 0, None),

        (880, '2024-01-22', 'A. Chen', 'pass', 0, None),

        (1000, '2024-04-10', 'R. Patel', 'pass', 0, None),

        (1356, '2023-07-25', 'S. Kim', 'fail', 2, 'Coal ash pond leak; missing monitoring logs'),

        (1356, '2024-06-01', 'S. Kim', 'conditional', 1, 'Ash pond repaired; logs still incomplete'),

        (2442, '2023-11-15', 'L. Johnson', 'pass', 0, None),

        (2442, '2024-05-30', 'L. Johnson', 'pass', 0, None),

        (3456, '2024-02-05', 'M. Davis', 'pass', 0, None),

        (7790, '2023-10-10', 'J. Martinez', 'pass', 0, None),

    ]


    cur.executemany(

        "INSERT INTO plant_inspections (plant_id_eia, inspection_date, inspector, result, violations, notes)"

        " VALUES (%s,%s,%s,%s,%s,%s)",

        inspections,

    )

    print(f"  plant_inspections:     {len(inspections)} rows")


    conn.commit()

    cur.close()

    conn.close()

    print("\nPostgreSQL PUDL reference database seeded successfully.")




if __name__ == "__main__":

    main()
