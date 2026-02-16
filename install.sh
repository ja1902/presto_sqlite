#!/usr/bin/env bash
#
# Interactive setup for presto-sqlite.
#
# Usage:
#   ./install.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PRESTO_VERSION="0.296"
PRESTO_TARBALL_URL="https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz"

# ── Presto ──────────────────────────────────────────────────────────────────────

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
    read -rp "Where should Presto be installed? (default: $SCRIPT_DIR): " install_dir
    if [ -z "$install_dir" ]; then
        install_dir="$SCRIPT_DIR"
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

# ── SQLite database ─────────────────────────────────────────────────────────────

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

# ── Step 1: Build ───────────────────────────────────────────────────────────────

echo ""
echo "Building presto-sqlite..."
cd "$SCRIPT_DIR"
./mvnw clean package -q
echo "Build complete."

# ── Step 2: Copy plugin JARs ────────────────────────────────────────────────────

PLUGIN_DIR="$PRESTO_HOME/plugin/sqlite"
if [ -d "$PLUGIN_DIR" ]; then
    echo "Removing existing plugin at $PLUGIN_DIR"
    rm -rf "$PLUGIN_DIR"
fi

mkdir -p "$PRESTO_HOME/plugin"
cp -r "$SCRIPT_DIR/target/presto-sqlite-0.296/sqlite" "$PLUGIN_DIR"
echo "Plugin installed to $PLUGIN_DIR"

# ── Step 3: Create catalog properties ───────────────────────────────────────────

CATALOG_DIR="$PRESTO_HOME/etc/catalog"
mkdir -p "$CATALOG_DIR"
cat > "$CATALOG_DIR/sqlite.properties" <<EOF
connector.name=sqlite
sqlite.db=$SQLITE_DB
EOF
echo "Catalog config written to $CATALOG_DIR/sqlite.properties"

# ── Step 4: Python venv ─────────────────────────────────────────────────────────

VENV_DIR="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip -q
    "$VENV_DIR/bin/pip" install prestodb -q
    echo "Python venv created at $VENV_DIR"
else
    echo "Python venv already exists at $VENV_DIR"
fi

# ── Step 5: Create demo database (if requested) ────────────────────────────────

if [ "$db_choice" = "2" ]; then
    echo "Creating demo SQLite database..."
    "$VENV_DIR/bin/python" "$SCRIPT_DIR/demo/create_sqlite_db.py"
    echo "Demo database created at $SQLITE_DB"
fi

# ── Done ────────────────────────────────────────────────────────────────────────

echo ""
echo "Done. Restart Presto, then run the demo:"
echo "  $VENV_DIR/bin/python demo/query_presto.py"
