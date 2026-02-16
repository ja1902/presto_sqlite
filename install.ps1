# Builds and installs the presto-sqlite plugin into a Presto installation.
#
# Usage:
#   .\install.ps1 -PrestoHome <PRESTO_HOME> -SqliteDb <SQLITE_DB_PATH>
#
# Example:
#   .\install.ps1 -PrestoHome C:\presto -SqliteDb C:\data\mydb.sqlite

param(
    [Parameter(Mandatory=$true)]
    [string]$PrestoHome,

    [Parameter(Mandatory=$true)]
    [string]$SqliteDb
)

$ErrorActionPreference = "Stop"

# Validate PRESTO_HOME
if (-not (Test-Path $PrestoHome)) {
    Write-Error "PrestoHome '$PrestoHome' does not exist."
    exit 1
}

if (-not (Test-Path "$PrestoHome\plugin")) {
    Write-Error "'$PrestoHome\plugin' not found. Is this a valid Presto installation?"
    exit 1
}

# Validate SQLite DB path is absolute
if (-not ([System.IO.Path]::IsPathRooted($SqliteDb))) {
    Write-Error "SqliteDb must be an absolute path (got '$SqliteDb')."
    exit 1
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Step 1: Build
Write-Host "Building presto-sqlite..."
Push-Location $ScriptDir
try {
    mvn clean package -q
    if ($LASTEXITCODE -ne 0) { throw "Maven build failed." }
} finally {
    Pop-Location
}
Write-Host "Build complete."

# Step 2: Copy plugin JARs
$PluginDir = Join-Path $PrestoHome "plugin\sqlite"
if (Test-Path $PluginDir) {
    Write-Host "Removing existing plugin at $PluginDir"
    Remove-Item -Recurse -Force $PluginDir
}

Copy-Item -Recurse "$ScriptDir\target\presto-sqlite-0.296\sqlite" $PluginDir
Write-Host "Plugin installed to $PluginDir"

# Step 3: Create catalog properties
$CatalogDir = Join-Path $PrestoHome "etc\catalog"
if (-not (Test-Path $CatalogDir)) {
    New-Item -ItemType Directory -Path $CatalogDir -Force | Out-Null
}

$PropsPath = Join-Path $CatalogDir "sqlite.properties"
@"
connector.name=sqlite
sqlite.db=$SqliteDb
"@ | Set-Content -Path $PropsPath -NoNewline

Write-Host "Catalog config written to $PropsPath"

# Step 4: Set up Python venv for demo scripts
$VenvDir = Join-Path $ScriptDir ".venv"
if (-not (Test-Path $VenvDir)) {
    Write-Host "Creating Python virtual environment..."
    python -m venv $VenvDir
    & "$VenvDir\Scripts\pip.exe" install --upgrade pip -q
    & "$VenvDir\Scripts\pip.exe" install prestodb -q
    Write-Host "Python venv created at $VenvDir"
} else {
    Write-Host "Python venv already exists at $VenvDir"
}

# Step 5: Create the demo database
Write-Host "Creating demo SQLite database..."
& "$VenvDir\Scripts\python.exe" "$ScriptDir\demo\create_sqlite_db.py"
Write-Host "Demo database created at $ScriptDir\demo\mock_data.db"

Write-Host ""
Write-Host "Done. Restart Presto, then run the demo:"
Write-Host "  $VenvDir\Scripts\python.exe demo\query_presto.py"
