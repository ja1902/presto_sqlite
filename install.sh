#!/usr/bin/env bash
#
# Interactive setup for presto-sqlite.
#
# Usage:
#   ./install.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# OS detection

case "$(uname -s)" in
    Darwin) OS_TYPE="macOS" ;;
    Linux)  OS_TYPE="Linux" ;;
    *)      OS_TYPE="$(uname -s)" ;;
esac
echo "Detected OS: $OS_TYPE"

# Prerequisites

if ! command -v java &>/dev/null; then
    echo "Error: Java is required but not found on PATH. Presto 0.296 requires Java 8+ (64-bit)."
    exit 1
fi
echo "Found Java: $(java -version 2>&1 | head -1)"

PRESTO_VERSION="0.296"
PRESTO_TARBALL_URL="https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"

# Presto

echo ""
echo "Do you already have Presto installed?"
echo "  1) Yes"
echo "  2) No"
read -rp "Enter 1 or 2: " presto_choice

if [ "$presto_choice" = "1" ]; then
    read -rp "Enter the path to your Presto installation: " PRESTO_HOME
    PRESTO_HOME="${PRESTO_HOME%\"}"
    PRESTO_HOME="${PRESTO_HOME#\"}"
    if [ ! -d "$PRESTO_HOME" ]; then
        echo "Error: '$PRESTO_HOME' does not exist."
        exit 1
    fi
    echo "Using existing Presto at $PRESTO_HOME"

elif [ "$presto_choice" = "2" ]; then
    if [ "$OS_TYPE" = "macOS" ]; then
        default_install_dir="$HOME/presto-server-$PRESTO_VERSION"
    else
        default_install_dir="$SCRIPT_DIR"
    fi
    read -rp "Where should Presto be installed? (default: $default_install_dir): " install_dir
    if [ -z "$install_dir" ]; then
        install_dir="$default_install_dir"
    fi
    mkdir -p "$install_dir"

    PRESTO_HOME="$install_dir/presto-server-$PRESTO_VERSION"

    if [ -d "$PRESTO_HOME" ]; then
        echo "Presto already exists at $PRESTO_HOME, skipping download."
    else
        tarfile="$install_dir/presto-server-$PRESTO_VERSION.tar.gz"
        echo "Downloading Presto $PRESTO_VERSION (~650 MB), this may take a few minutes..."
        if command -v curl &>/dev/null; then
            curl -fSL -o "$tarfile" "$PRESTO_TARBALL_URL"
        elif command -v wget &>/dev/null; then
            wget -q -O "$tarfile" "$PRESTO_TARBALL_URL"
        else
            echo "Error: curl or wget is required to download Presto."
            exit 1
        fi
        echo "Download complete. Extracting..."
        tar -xf "$tarfile" -C "$install_dir"
        rm -f "$tarfile"
        echo "Presto extracted to $PRESTO_HOME"
    fi

    # Create default config files if they don't exist
    mkdir -p "$PRESTO_HOME/etc"

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

    if [ ! -f "$PRESTO_HOME/etc/log.properties" ]; then
        cat > "$PRESTO_HOME/etc/log.properties" <<'EOF'
com.facebook.presto=INFO
EOF
    fi

    echo "Presto $PRESTO_VERSION is ready at $PRESTO_HOME"
else
    echo "Invalid choice. Please enter 1 or 2."
    exit 1
fi

# SQLite database

echo ""
echo "Do you already have a SQLite database?"
echo "  1) Yes"
echo "  2) No, create a sample database for me"
read -rp "Enter 1 or 2: " db_choice

if [ "$db_choice" = "1" ]; then
    read -rp "Enter the absolute path to your .db / .sqlite file: " SQLITE_DB
    SQLITE_DB="${SQLITE_DB%\"}"
    SQLITE_DB="${SQLITE_DB#\"}"
    case "$SQLITE_DB" in
        /*) ;;
        *)
            echo "Error: path must be absolute (got '$SQLITE_DB')."
            exit 1
            ;;
    esac
    if [ ! -f "$SQLITE_DB" ]; then
        echo "Error: '$SQLITE_DB' does not exist."
        exit 1
    fi
    echo "Using database at $SQLITE_DB"

elif [ "$db_choice" = "2" ]; then
    SQLITE_DB="$SCRIPT_DIR/demo/mock_data.db"
    echo "A sample database will be created at $SQLITE_DB"
else
    echo "Invalid choice. Please enter 1 or 2."
    exit 1
fi

# Step 1: Build

# Rewrite maven-wrapper.properties to ensure LF line endings.
mkdir -p "$SCRIPT_DIR/.mvn/wrapper"
cat > "$SCRIPT_DIR/.mvn/wrapper/maven-wrapper.properties" <<'MEOF'
wrapperVersion=3.3.4
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip
MEOF

echo ""
echo "Building presto-sqlite..."
cd "$SCRIPT_DIR"
bash mvnw clean package -q
echo "Build complete."

# Step 2: Copy plugin JARs

PLUGIN_DIR="$PRESTO_HOME/plugin/sqlite"
if [ -d "$PLUGIN_DIR" ]; then
    echo "Removing existing plugin at $PLUGIN_DIR"
    rm -rf "$PLUGIN_DIR"
fi

mkdir -p "$PRESTO_HOME/plugin"
cp -r "$SCRIPT_DIR/target/presto-sqlite-0.296/sqlite" "$PLUGIN_DIR"
echo "Plugin installed to $PLUGIN_DIR"

# Step 3: Create catalog properties

CATALOG_DIR="$PRESTO_HOME/etc/catalog"
mkdir -p "$CATALOG_DIR"
cat > "$CATALOG_DIR/sqlite.properties" <<EOF
connector.name=sqlite
sqlite.db=$SQLITE_DB
EOF
echo "Catalog config written to $CATALOG_DIR/sqlite.properties"

# Step 4: Python venv

# On WSL with /mnt paths, put the venv on the native Linux filesystem
# to avoid the I/O penalty of cross-filesystem writes.
case "$SCRIPT_DIR" in
    /mnt/*) VENV_DIR="$HOME/.presto_sqlite_venv" ;;
    *)      VENV_DIR="$SCRIPT_DIR/.venv" ;;
esac
echo "Python venv: $VENV_DIR"

# If a Windows venv exists (has Scripts\ but no bin/), delete and recreate it.
if [ -d "$VENV_DIR" ] && [ ! -f "$VENV_DIR/bin/python" ]; then
    echo "Removing Windows venv and recreating for Linux..."
    rm -rf "$VENV_DIR"
fi
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip -q
    "$VENV_DIR/bin/pip" install presto-python-client psycopg2-binary -q
    echo "Python venv created at $VENV_DIR"
else
    echo "Python venv already exists at $VENV_DIR"
    "$VENV_DIR/bin/pip" install psycopg2-binary -q
fi

# Step 5: Create demo database (if requested)

if [ "$db_choice" = "2" ]; then
    echo "Creating demo SQLite database..."
    "$VENV_DIR/bin/python" "$SCRIPT_DIR/demo/create_sqlite_db.py"
    echo "Demo database created at $SQLITE_DB"
fi

# Step 6: Start Presto

LAUNCHER="$PRESTO_HOME/bin/launcher"
if [ -f "$LAUNCHER" ]; then
    echo ""
    echo "Starting Presto server..."
    "$LAUNCHER" start

    # Wait for Presto to be ready
    echo "Waiting for Presto to be ready (this may take 15-30 seconds)..."
    ready=false
    for i in $(seq 1 60); do
        sleep 2
        if curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; then
            ready=true
            break
        fi
    done

    if [ "$ready" = true ]; then
        echo "Presto is running at http://localhost:8080"
    else
        echo "WARNING: Presto did not respond within 2 minutes."
        echo "Check the logs at: $PRESTO_HOME/data/var/log"
    fi
else
    echo ""
    echo "WARNING: Could not find Presto launcher at $LAUNCHER"
    echo "You will need to start Presto manually before querying."
fi

# Done

echo ""
echo "Done."
echo ""
echo "Activate the virtual environment:"
echo "  source $VENV_DIR/bin/activate"
echo ""
echo "Run the demo:"
echo "  python demo/query_presto.py"
echo ""
echo "Start/stop Presto:"
echo "  $LAUNCHER start"
echo "  $LAUNCHER stop"
