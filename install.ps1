# Interactive setup for presto-sqlite.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File install.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Prerequisites

# Find Java -- check PATH first, then fall back to common install locations.
$JavaExe = $null
if (Get-Command java -ErrorAction SilentlyContinue) {
    $JavaExe = "java"
} else {
    $SearchRoots = @(
        "C:\Program Files\Java",
        "C:\Program Files\Eclipse Adoptium",
        "C:\Program Files\Microsoft",
        "C:\Program Files\Amazon Corretto",
        "C:\Program Files\BellSoft",
        "C:\Program Files\Zulu",
        "$env:LOCALAPPDATA\Programs\Eclipse Adoptium"
    )
    foreach ($root in $SearchRoots) {
        if (Test-Path $root) {
            $found = Get-ChildItem $root -Filter "java.exe" -Recurse -ErrorAction SilentlyContinue |
                     Where-Object { $_.FullName -notlike "*jre*" } |
                     Sort-Object FullName -Descending |
                     Select-Object -First 1
            if (-not $found) {
                # also accept jre java.exe
                $found = Get-ChildItem $root -Filter "java.exe" -Recurse -ErrorAction SilentlyContinue |
                         Sort-Object FullName -Descending |
                         Select-Object -First 1
            }
            if ($found) {
                $JavaExe = $found.FullName
                $JavaBin = Split-Path $found.FullName -Parent
                $env:PATH = "$JavaBin;$env:PATH"
                $env:JAVA_HOME = Split-Path $JavaBin -Parent
                Write-Host "Java found at $JavaExe (added to PATH for this session)"
                break
            }
        }
    }
}

if (-not $JavaExe) {
    Write-Error "Java not found. Please install Java 8+ (64-bit) and re-run this script.`nDownload: https://adoptium.net"
    exit 1
}

# java -version always writes to stderr; use a local scope to avoid triggering
# $ErrorActionPreference = "Stop" on the stderr output.
$javaVersion = & { $ErrorActionPreference = 'Continue'; & $JavaExe -version 2>&1 } | Select-Object -First 1
Write-Host "Found Java: $javaVersion"

$PRESTO_VERSION = "0.296"

# Docker check

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is required but not found on PATH.`nInstall Docker Desktop from https://www.docker.com/products/docker-desktop and re-run."
    exit 1
}
$dockerInfo = & { $ErrorActionPreference = 'Continue'; & docker info 2>&1 }
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker is installed but not running. Start Docker Desktop and re-run."
    exit 1
}
Write-Host "Docker is available."

# SQLite database

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

# Step 1: Build

$WrapperDir = Join-Path $ScriptDir ".mvn\wrapper"
if (-not (Test-Path (Join-Path $WrapperDir "maven-wrapper.properties"))) {
    New-Item -ItemType Directory -Path $WrapperDir -Force | Out-Null
    @"
wrapperVersion=3.3.4
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip
"@ | Set-Content -Path (Join-Path $WrapperDir "maven-wrapper.properties") -NoNewline
}

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

# Step 2: Create catalog config (mounted into the Docker container at runtime)

$CatalogDir = Join-Path $ScriptDir "etc\catalog"
if (-not (Test-Path $CatalogDir)) {
    New-Item -ItemType Directory -Path $CatalogDir -Force | Out-Null
}

$PropsPath = Join-Path $CatalogDir "sqlite.properties"
@"
connector.name=sqlite
sqlite.db=/data/sqlite.db
"@ | Set-Content -Path $PropsPath -NoNewline

Write-Host "Catalog config written to $PropsPath"

$PostgresPropsPath = Join-Path $CatalogDir "postgres.properties"
@"
connector.name=postgresql
connection-url=jdbc:postgresql://presto-postgres:5432/demo
connection-user=presto
connection-password=presto
"@ | Set-Content -Path $PostgresPropsPath -NoNewline

Write-Host "Catalog config written to $PostgresPropsPath"

# Step 3: Python venv

$VenvDir = Join-Path $ScriptDir ".venv"
$VenvPip = Join-Path $VenvDir "Scripts\pip.exe"
$VenvValid = (Test-Path $VenvDir) -and (Test-Path $VenvPip)

if (-not $VenvValid) {
    if (Test-Path $VenvDir) {
        Write-Host "Existing venv is incomplete, recreating..."
        Remove-Item -Recurse -Force $VenvDir
    }
    Write-Host "Creating Python virtual environment..."
    python -m venv $VenvDir
    & $VenvPip install --upgrade pip -q
    & $VenvPip install presto-python-client psycopg2-binary -q
    Write-Host "Python venv created at $VenvDir"
} else {
    Write-Host "Python venv already exists at $VenvDir"
    & $VenvPip install psycopg2-binary -q
}

# Step 4: Create demo database (if requested)

if ($dbChoice -eq "2") {
    Write-Host "Creating demo SQLite database..."
    & "$VenvDir\Scripts\python.exe" "$ScriptDir\demo\create_sqlite_db.py"
    Write-Host "Demo database created at $SqliteDb"
}

# Step 5: Start PostgreSQL via Docker

# Ensure the shared Docker network exists (silently skip if it already does).
& { $ErrorActionPreference = 'Continue'; & docker network create presto-net 2>&1 | Out-Null }

# Remove any existing postgres container so we get a clean start.
& { $ErrorActionPreference = 'Continue'; & docker rm -f presto-postgres 2>&1 | Out-Null }

Write-Host ""
Write-Host "Starting PostgreSQL in Docker..."
& docker run -d --name presto-postgres --network presto-net `
    -e POSTGRES_DB=demo -e POSTGRES_USER=presto -e POSTGRES_PASSWORD=presto `
    -p 5432:5432 `
    postgres:16

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to start PostgreSQL container. Make sure Docker Desktop is running."
    exit 1
}

Write-Host "Waiting for PostgreSQL to be ready..."
$pgReady = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    $pgCheck = & { $ErrorActionPreference = 'Continue'; & docker exec presto-postgres pg_isready -U presto 2>&1 }
    if ($LASTEXITCODE -eq 0) { $pgReady = $true; break }
}

if (-not $pgReady) {
    Write-Error "PostgreSQL did not become ready in time.`nCheck logs with: docker logs presto-postgres"
    exit 1
}

Write-Host "PostgreSQL is ready."
Write-Host "Seeding PostgreSQL demo database..."
& "$VenvDir\Scripts\python.exe" "$ScriptDir\demo\create_postgres_db.py"

# Step 6: Start Presto via Docker

$PluginSrc = "$ScriptDir\target\presto-sqlite-$PRESTO_VERSION\sqlite"

# Remove any existing container so we get a clean start (ignore error if it doesn't exist).
& { $ErrorActionPreference = 'Continue'; & docker rm -f presto 2>&1 | Out-Null }

Write-Host ""
Write-Host "Starting Presto $PRESTO_VERSION via Docker..."
& docker run -d --name presto -p 8080:8080 --network presto-net `
    -v "${PluginSrc}:/opt/presto-server/plugin/sqlite" `
    -v "${PropsPath}:/opt/presto-server/etc/catalog/sqlite.properties" `
    -v "${PostgresPropsPath}:/opt/presto-server/etc/catalog/postgres.properties" `
    -v "${SqliteDb}:/data/sqlite.db:ro" `
    "prestodb/presto:$PRESTO_VERSION"

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to start Docker container. Make sure Docker Desktop is running."
    exit 1
}

Write-Host "Waiting for Presto to be ready (first start pulls the image and may take a few minutes)..."
$ready = $false
$startTime = Get-Date
for ($i = 0; $i -lt 90; $i++) {
    Start-Sleep -Seconds 2
    $elapsed = [int]((Get-Date) - $startTime).TotalSeconds
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/v1/info" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            $ready = $true
            Write-Host ""
            break
        }
    } catch { }
    Write-Host -NoNewline "`r  Still waiting... ${elapsed}s elapsed"
}

if ($ready) {
    Write-Host "Presto is running at http://localhost:8080"
} else {
    Write-Host ""
    Write-Host "WARNING: Presto did not respond after 3 minutes."
    Write-Host "Check container logs with:  docker logs presto"
}

# Done

Write-Host ""
Write-Host "Done. Presto is running in Docker."
Write-Host ""
Write-Host "Activate the virtual environment in your shell:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "Then run the demo:"
Write-Host "  python demo\query_presto.py"
Write-Host ""
Write-Host "  (or without activating: $VenvDir\Scripts\python.exe demo\query_presto.py)"
Write-Host ""
Write-Host "To start/stop Presto and PostgreSQL in the future:"
Write-Host "  docker start presto-postgres presto"
Write-Host "  docker stop presto presto-postgres"