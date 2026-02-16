#!/usr/bin/env bash
#
# Builds and installs the presto-sqlite plugin into a Presto installation.
#
# Usage:
#   ./install.sh <PRESTO_HOME> <SQLITE_DB_PATH>
#
# Example:
#   ./install.sh /opt/presto /data/mydb.sqlite
#

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <PRESTO_HOME> <SQLITE_DB_PATH>"
    echo ""
    echo "  PRESTO_HOME     Root of your Presto installation"
    echo "  SQLITE_DB_PATH  Absolute path to the SQLite database file"
    exit 1
fi

PRESTO_HOME="$1"
SQLITE_DB="$2"

# Validate PRESTO_HOME
if [ ! -d "$PRESTO_HOME" ]; then
    echo "Error: PRESTO_HOME '$PRESTO_HOME' does not exist."
    exit 1
fi

if [ ! -d "$PRESTO_HOME/plugin" ]; then
    echo "Error: '$PRESTO_HOME/plugin' not found. Is this a valid Presto installation?"
    exit 1
fi

# Validate SQLite DB path is absolute
case "$SQLITE_DB" in
    /*) ;;
    *)
        echo "Error: SQLITE_DB_PATH must be an absolute path (got '$SQLITE_DB')."
        exit 1
        ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Step 1: Build
echo "Building presto-sqlite..."
cd "$SCRIPT_DIR"
mvn clean package -q
echo "Build complete."

# Step 2: Copy plugin JARs
PLUGIN_DIR="$PRESTO_HOME/plugin/sqlite"
if [ -d "$PLUGIN_DIR" ]; then
    echo "Removing existing plugin at $PLUGIN_DIR"
    rm -rf "$PLUGIN_DIR"
fi

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

# Step 4: Set up Python venv for demo scripts
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

# Step 5: Create the demo database
echo "Creating demo SQLite database..."
"$VENV_DIR/bin/python" "$SCRIPT_DIR/demo/create_sqlite_db.py"
echo "Demo database created at $SCRIPT_DIR/demo/mock_data.db"

echo ""
echo "Done. Restart Presto, then run the demo:"
echo "  $VENV_DIR/bin/python demo/query_presto.py"
