# Interactive setup for presto-sqlite.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File install.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$PRESTO_VERSION = "0.296"
$PRESTO_TARBALL_URL = "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/$PRESTO_VERSION/presto-server-$PRESTO_VERSION.tar.gz"

# ── Presto ──────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "Do you already have Presto installed?"
Write-Host "  1) Yes"
Write-Host "  2) No"
$prestoChoice = Read-Host "Enter 1 or 2"

if ($prestoChoice -eq "1") {
    $PrestoHome = Read-Host "Enter the path to your Presto installation"
    $PrestoHome = $PrestoHome.Trim('"').Trim("'")
    if (-not (Test-Path $PrestoHome)) {
        Write-Error "Path '$PrestoHome' does not exist."
        exit 1
    }
    Write-Host "Using existing Presto at $PrestoHome"
}
elseif ($prestoChoice -eq "2") {
    $InstallDir = Read-Host "Where should Presto be installed? (default: $ScriptDir)"
    if ([string]::IsNullOrWhiteSpace($InstallDir)) {
        $InstallDir = $ScriptDir
    }
    $InstallDir = $InstallDir.Trim('"').Trim("'")
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }

    $PrestoHome = Join-Path $InstallDir "presto-server-$PRESTO_VERSION"

    if (Test-Path $PrestoHome) {
        Write-Host "Presto already exists at $PrestoHome, skipping download."
    }
    else {
        $TarFile = Join-Path $InstallDir "presto-server-$PRESTO_VERSION.tar.gz"
        Write-Host "Downloading Presto $PRESTO_VERSION (~650 MB), this may take a few minutes..."
        Invoke-WebRequest -Uri $PRESTO_TARBALL_URL -OutFile $TarFile -UseBasicParsing
        Write-Host "Download complete. Extracting..."
        tar -xf $TarFile -C $InstallDir
        Remove-Item $TarFile
        Write-Host "Presto extracted to $PrestoHome"
    }

    # Create default config files if they don't exist
    $EtcDir = Join-Path $PrestoHome "etc"
    if (-not (Test-Path $EtcDir)) {
        New-Item -ItemType Directory -Path $EtcDir -Force | Out-Null
    }

    $NodeProps = Join-Path $EtcDir "node.properties"
    if (-not (Test-Path $NodeProps)) {
        $DataDir = Join-Path $PrestoHome "data"
        @"
node.environment=production
node.id=$(New-Guid)
node.data-dir=$DataDir
"@ | Set-Content -Path $NodeProps -NoNewline
    }

    $JvmConfig = Join-Path $EtcDir "jvm.config"
    if (-not (Test-Path $JvmConfig)) {
        @"
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
"@ | Set-Content -Path $JvmConfig -NoNewline
    }

    $ConfigProps = Join-Path $EtcDir "config.properties"
    if (-not (Test-Path $ConfigProps)) {
        @"
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://localhost:8080
"@ | Set-Content -Path $ConfigProps -NoNewline
    }

    $LogProps = Join-Path $EtcDir "log.properties"
    if (-not (Test-Path $LogProps)) {
        @"
com.facebook.presto=INFO
"@ | Set-Content -Path $LogProps -NoNewline
    }

    Write-Host "Presto $PRESTO_VERSION is ready at $PrestoHome"
}
else {
    Write-Error "Invalid choice. Please enter 1 or 2."
    exit 1
}

# ── SQLite database ─────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "Do you already have a SQLite database?"
Write-Host "  1) Yes"
Write-Host "  2) No, create a sample database for me"
$dbChoice = Read-Host "Enter 1 or 2"

if ($dbChoice -eq "1") {
    $SqliteDb = Read-Host "Enter the absolute path to your .db / .sqlite file"
    $SqliteDb = $SqliteDb.Trim('"').Trim("'")
    if (-not ([System.IO.Path]::IsPathRooted($SqliteDb))) {
        Write-Error "Path must be absolute (got '$SqliteDb')."
        exit 1
    }
    if (-not (Test-Path $SqliteDb)) {
        Write-Error "File '$SqliteDb' does not exist."
        exit 1
    }
    Write-Host "Using database at $SqliteDb"
}
elseif ($dbChoice -eq "2") {
    $SqliteDb = Join-Path $ScriptDir "demo\mock_data.db"
    Write-Host "A sample database will be created at $SqliteDb"
}
else {
    Write-Error "Invalid choice. Please enter 1 or 2."
    exit 1
}

# ── Step 1: Build ───────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "Building presto-sqlite..."
Push-Location $ScriptDir
try {
    & "$ScriptDir\mvnw.cmd" clean package -q
    if ($LASTEXITCODE -ne 0) { throw "Maven build failed." }
} finally {
    Pop-Location
}
Write-Host "Build complete."

# ── Step 2: Copy plugin JARs ────────────────────────────────────────────────────

$PluginParent = Join-Path $PrestoHome "plugin"
if (-not (Test-Path $PluginParent)) {
    New-Item -ItemType Directory -Path $PluginParent -Force | Out-Null
}

$PluginDir = Join-Path $PluginParent "sqlite"
if (Test-Path $PluginDir) {
    Write-Host "Removing existing plugin at $PluginDir"
    Remove-Item -Recurse -Force $PluginDir
}

Copy-Item -Recurse "$ScriptDir\target\presto-sqlite-0.296\sqlite" $PluginDir
Write-Host "Plugin installed to $PluginDir"

# ── Step 3: Create catalog properties ───────────────────────────────────────────

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

# ── Step 4: Python venv ─────────────────────────────────────────────────────────

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

# ── Step 5: Create demo database (if requested) ────────────────────────────────

if ($dbChoice -eq "2") {
    Write-Host "Creating demo SQLite database..."
    & "$VenvDir\Scripts\python.exe" "$ScriptDir\demo\create_sqlite_db.py"
    Write-Host "Demo database created at $SqliteDb"
}

# ── Done ────────────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "Done. Restart Presto, then run the demo:"
Write-Host "  $VenvDir\Scripts\python.exe demo\query_presto.py"
