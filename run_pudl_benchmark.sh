#!/usr/bin/env bash
#
# Automated PUDL benchmark for presto-sqlite.
#
# Downloads the 21 GB PUDL energy database, builds the connector, starts
# Presto + PostgreSQL, and runs the full benchmark + cross-connector test
# suite.  One command, no prompts.
#
# Prerequisites: Java 8+, Docker (running), Python 3, AWS CLI v2
#
# Usage:
#   chmod +x run_pudl_benchmark.sh
#   ./run_pudl_benchmark.sh
#
# Options (environment variables):
#   PUDL_DB="/path/to/pudl.sqlite"   # skip download, use existing DB
#   SKIP_BUILD=1                     # skip Maven build
#   SKIP_BENCHMARK=1                 # skip SQLite benchmark, run cross-connector only
#   BENCHMARK_RUNS=3                 # recorded runs per query (default: 3)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRESTO_VERSION="0.296"

echo ""
echo "============================================================"
echo "  PUDL Benchmark - Presto SQLite Connector"
echo "============================================================"
echo ""
echo "  This script will:"
echo "    1. Check prerequisites (Java, Docker, Python, AWS CLI)"
echo "    2. Build the presto-sqlite connector"
echo "    3. Download the PUDL database (~3.7 GB zip, ~21 GB unzipped)"
echo "    4. Start PostgreSQL + Presto"
echo "    5. Run the full benchmark suite"
echo "    6. Run the cross-connector federation test"
echo ""

# ── Step 1: Prerequisites ─────────────────────────────────────────────────────

echo "Step 1: Checking prerequisites..."
echo ""

if ! command -v java &>/dev/null; then
    echo "ERROR: Java not found. Install Java 8+ (64-bit) from https://adoptium.net"
    exit 1
fi
echo "  [OK] Java: $(java -version 2>&1 | head -1)"

if ! command -v docker &>/dev/null; then
    echo "  [WARN] Docker not found. Cross-connector test will fail."
elif ! docker info &>/dev/null; then
    echo "  [WARN] Docker is installed but not running. Cross-connector test will fail."
else
    echo "  [OK] Docker is running"
fi

if ! command -v python3 &>/dev/null; then
    echo "ERROR: Python 3 not found."
    exit 1
fi
echo "  [OK] $(python3 --version)"

if ! command -v aws &>/dev/null; then
    echo "ERROR: AWS CLI not found. Install from https://aws.amazon.com/cli/"
    exit 1
fi
echo "  [OK] $(aws --version 2>&1 | head -1)"

echo ""

# ── Step 2: Build ─────────────────────────────────────────────────────────────

if [ "${SKIP_BUILD:-}" = "1" ]; then
    echo "Step 2: Skipping build (SKIP_BUILD=1)"
else
    echo "Step 2: Building presto-sqlite connector..."

    mkdir -p "$SCRIPT_DIR/.mvn/wrapper"
    cat > "$SCRIPT_DIR/.mvn/wrapper/maven-wrapper.properties" <<'MEOF'
wrapperVersion=3.3.4
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip
MEOF

    cd "$SCRIPT_DIR"
    if command -v mvn &>/dev/null; then
        mvn clean package -q
    else
        bash mvnw clean package -q
    fi
    echo "  Build complete."
fi
echo ""

# ── Step 3: Download PUDL database ────────────────────────────────────────────

DATA_DIR="$SCRIPT_DIR/data"
PUDL_ZIP="$DATA_DIR/pudl.sqlite.zip"
PUDL_DB="${PUDL_DB:-$DATA_DIR/pudl.sqlite}"

if [ -n "${PUDL_DB:-}" ] && [ -f "$PUDL_DB" ]; then
    db_size=$(du -h "$PUDL_DB" | cut -f1)
    echo "Step 3: PUDL database exists at $PUDL_DB ($db_size) -- skipping download"
else
    echo "Step 3: Downloading PUDL database..."
    echo "  Source: s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip"
    echo "  This is ~3.7 GB compressed, ~21 GB uncompressed."
    echo ""

    mkdir -p "$DATA_DIR"

    # Check disk space
    if command -v df &>/dev/null; then
        avail_gb=$(df -BG "$DATA_DIR" 2>/dev/null | tail -1 | awk '{print $4}' | tr -d 'G' || echo "999")
        if [ "${avail_gb:-999}" -lt 25 ] 2>/dev/null; then
            echo "ERROR: Not enough disk space. Need ~25 GB, have ~${avail_gb} GB."
            exit 1
        fi
        echo "  Disk space: ${avail_gb} GB free"
    fi

    echo "  Downloading..."
    dl_start=$(date +%s)
    aws s3 cp --no-sign-request s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip "$PUDL_ZIP"
    dl_end=$(date +%s)
    echo "  Downloaded in $((dl_end - dl_start))s"

    echo "  Extracting (this may take a few minutes)..."
    extract_start=$(date +%s)
    cd "$DATA_DIR"
    unzip -o "$PUDL_ZIP"
    extract_end=$(date +%s)
    echo "  Extracted in $((extract_end - extract_start))s"
    cd "$SCRIPT_DIR"

    rm -f "$PUDL_ZIP"
    echo "  Removed zip file to save space."

    # Find the extracted database
    PUDL_DB="$DATA_DIR/pudl.sqlite"
    if [ ! -f "$PUDL_DB" ]; then
        PUDL_DB=$(find "$DATA_DIR" -name "*.sqlite" -o -name "*.db" | head -1)
        if [ -z "$PUDL_DB" ]; then
            echo "ERROR: Could not find extracted database in $DATA_DIR"
            exit 1
        fi
    fi
fi

db_size=$(du -h "$PUDL_DB" | cut -f1)
echo "  PUDL database: $PUDL_DB ($db_size)"
echo ""

# ── Step 4: Python venv ───────────────────────────────────────────────────────

echo "Step 4: Setting up Python environment..."

case "$SCRIPT_DIR" in
    /mnt/*) VENV_DIR="$HOME/.presto_sqlite_venv" ;;
    *)      VENV_DIR="$SCRIPT_DIR/.venv" ;;
esac

if [ -d "$VENV_DIR" ] && [ ! -f "$VENV_DIR/bin/python" ]; then
    rm -rf "$VENV_DIR"
fi
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip -q
    "$VENV_DIR/bin/pip" install presto-python-client psycopg2-binary -q
    echo "  Python venv created."
else
    "$VENV_DIR/bin/pip" install presto-python-client psycopg2-binary -q 2>/dev/null
    echo "  Python venv ready."
fi

PYTHON="$VENV_DIR/bin/python"
echo ""

# ── Step 5: Presto installation ───────────────────────────────────────────────

echo "Step 5: Setting up Presto $PRESTO_VERSION..."

PRESTO_HOME="$SCRIPT_DIR/presto-server-$PRESTO_VERSION"

if [ ! -d "$PRESTO_HOME" ]; then
    tarfile="$SCRIPT_DIR/presto-server-$PRESTO_VERSION.tar.gz"
    echo "  Downloading Presto $PRESTO_VERSION (~650 MB)..."
    if command -v curl &>/dev/null; then
        curl -fSL -o "$tarfile" "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"
    else
        wget -q -O "$tarfile" "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"
    fi
    tar -xf "$tarfile" -C "$SCRIPT_DIR"
    rm -f "$tarfile"
fi

# Install plugin
PLUGIN_DIR="$PRESTO_HOME/plugin/sqlite"
rm -rf "$PLUGIN_DIR" 2>/dev/null || true
mkdir -p "$PRESTO_HOME/plugin"
cp -r "$SCRIPT_DIR/target/presto-sqlite-$PRESTO_VERSION/sqlite" "$PLUGIN_DIR"
echo "  Plugin installed."

# Config files
mkdir -p "$PRESTO_HOME/etc/catalog"

if [ ! -f "$PRESTO_HOME/etc/node.properties" ]; then
    cat > "$PRESTO_HOME/etc/node.properties" <<EOF
node.environment=production
node.id=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
node.data-dir=$PRESTO_HOME/data
EOF
fi

if [ ! -f "$PRESTO_HOME/etc/jvm.config" ]; then
    cat > "$PRESTO_HOME/etc/jvm.config" <<'EOF'
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
EOF
fi

if [ ! -f "$PRESTO_HOME/etc/config.properties" ]; then
    cat > "$PRESTO_HOME/etc/config.properties" <<'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://localhost:8080
EOF
fi

cat > "$PRESTO_HOME/etc/catalog/sqlite.properties" <<EOF
connector.name=sqlite
sqlite.db=$PUDL_DB
EOF

# ── Step 6: Start PostgreSQL ──────────────────────────────────────────────────

echo ""
echo "Step 6: Starting PostgreSQL..."

if command -v docker &>/dev/null && docker info &>/dev/null; then
    docker network create presto-net 2>/dev/null || true

    pg_running=$(docker inspect -f '{{.State.Running}}' presto-postgres 2>/dev/null || echo "false")
    if [ "$pg_running" = "true" ]; then
        echo "  PostgreSQL container already running."
    else
        docker rm -f presto-postgres 2>/dev/null || true

        docker run -d --name presto-postgres --network presto-net \
            -e POSTGRES_DB=demo -e POSTGRES_USER=presto -e POSTGRES_PASSWORD=presto \
            -p 5432:5432 \
            postgres:16

        echo "  Waiting for PostgreSQL..."
        for i in $(seq 1 30); do
            sleep 2
            if docker exec presto-postgres pg_isready -U presto &>/dev/null; then
                break
            fi
        done
        echo "  PostgreSQL is ready."
    fi

    echo "  Seeding PostgreSQL with PUDL reference data..."
    "$PYTHON" "$SCRIPT_DIR/demo/create_postgres_pudl.py"

    cat > "$PRESTO_HOME/etc/catalog/postgres.properties" <<'EOF'
connector.name=postgresql
connection-url=jdbc:postgresql://localhost:5432/demo
connection-user=presto
connection-password=presto
EOF
fi
echo ""

# ── Step 7: Start Presto ──────────────────────────────────────────────────────

echo "Step 7: Starting Presto..."

# Stop if already running
if curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  Stopping existing Presto instance..."
    "$PRESTO_HOME/bin/launcher" stop 2>/dev/null || true
    sleep 3
fi

"$PRESTO_HOME/bin/launcher" start

echo "  Waiting for Presto to be ready..."
ready=false
for i in $(seq 1 60); do
    sleep 2
    if curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; then
        ready=true
        break
    fi
done

if [ "$ready" = true ]; then
    echo "  Presto is running at http://localhost:8080"
else
    echo "ERROR: Presto did not respond within 2 minutes."
    echo "Check logs at: $PRESTO_HOME/data/var/log"
    exit 1
fi

sleep 5

echo "  Verifying catalogs..."
"$PYTHON" -c "
import prestodb, sys
conn = prestodb.dbapi.connect(host='localhost', port=8080, user='test')
cur = conn.cursor()
cur.execute('SHOW CATALOGS')
catalogs = [r[0] for r in cur.fetchall()]
print('  Catalogs: ' + ', '.join(catalogs))
if 'sqlite' not in catalogs:
    print('ERROR: sqlite catalog not found', file=sys.stderr)
    sys.exit(1)
cur.close()
conn.close()
"
echo ""

# ── Step 8: Run SQLite benchmark ──────────────────────────────────────────────

BENCHMARK_RUNS="${BENCHMARK_RUNS:-3}"

if [ "${SKIP_BENCHMARK:-}" = "1" ]; then
    echo "Step 8: Skipping SQLite benchmark (SKIP_BENCHMARK=1)"
else
    echo "Step 8: Running SQLite benchmark (PUDL database)..."
    echo "  Tables: 19K to 3.3M rows | Runs: $BENCHMARK_RUNS per query"
    echo ""
    echo "------------------------------------------------------------"

    PYTHONIOENCODING=utf-8 "$PYTHON" "$SCRIPT_DIR/demo/benchmark_pudl.py" \
        --large --runs "$BENCHMARK_RUNS" --warmup 1 --timeout 120

    echo "------------------------------------------------------------"
fi
echo ""

# ── Step 9: Run cross-connector test ──────────────────────────────────────────

echo "Step 9: Running cross-connector federation test (SQLite + PostgreSQL)..."
echo ""
echo "------------------------------------------------------------"

PYTHONIOENCODING=utf-8 "$PYTHON" "$SCRIPT_DIR/demo/cross_connector_test.py"

echo "------------------------------------------------------------"
echo ""

# ── Done ──────────────────────────────────────────────────────────────────────

echo "============================================================"
echo "  PUDL Benchmark Complete"
echo "============================================================"
echo ""
echo "  PUDL database:     $PUDL_DB ($db_size)"
echo "  Presto:            http://localhost:8080"
echo "  PostgreSQL:        localhost:5432 (presto/presto)"
echo ""
echo "  Re-run benchmarks:"
echo "    $PYTHON demo/benchmark_pudl.py --large"
echo "    $PYTHON demo/cross_connector_test.py"
echo ""
echo "  For detailed analysis see: BENCHMARK_REPORT.md"
echo ""
echo "  Start/stop Presto:"
echo "    $PRESTO_HOME/bin/launcher start"
echo "    $PRESTO_HOME/bin/launcher stop"
echo ""
echo "  Start/stop PostgreSQL:"
echo "    docker start presto-postgres"
echo "    docker stop presto-postgres"
echo ""
