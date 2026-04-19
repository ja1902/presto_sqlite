# Automated PUDL benchmark for presto-sqlite.

#

# Downloads the 21 GB PUDL energy database, builds the connector, starts

# Presto + PostgreSQL in Docker, and runs the full benchmark + cross-connector

# test suite.  One command, no prompts.

#

# Prerequisites: Java 8+, Docker Desktop (running), Python 3, AWS CLI v2

#

# Usage:

#   powershell -ExecutionPolicy Bypass -File run_pudl_benchmark.ps1

#

# Options (environment variables):

#   $env:PUDL_DB = "C:\path\to\pudl.sqlite"   # skip download, use existing DB

#   $env:SKIP_BUILD = "1"                      # skip Maven build

#   $env:SKIP_BENCHMARK = "1"                  # skip SQLite benchmark, run cross-connector only

#   $env:BENCHMARK_RUNS = "3"                  # recorded runs per query (default: 3)


$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$PRESTO_VERSION = "0.296"


Write-Host ""

Write-Host "============================================================"

Write-Host "  PUDL Benchmark - Presto SQLite Connector"

Write-Host "============================================================"

Write-Host ""

Write-Host "  This script will:"

Write-Host "    1. Check prerequisites (Java, Docker, Python, AWS CLI)"

Write-Host "    2. Build the presto-sqlite connector"

Write-Host "    3. Download the PUDL database (~3.7 GB zip, ~21 GB unzipped)"

Write-Host "    4. Start PostgreSQL + Presto in Docker"

Write-Host "    5. Run the full benchmark suite"

Write-Host "    6. Run the cross-connector federation test"

Write-Host ""


# ── Step 1: Prerequisites ─────────────────────────────────────────────────────


Write-Host "Step 1: Checking prerequisites..."

Write-Host ""


# Java

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

                $found = Get-ChildItem $root -Filter "java.exe" -Recurse -ErrorAction SilentlyContinue |

                         Sort-Object FullName -Descending |

                         Select-Object -First 1

            }

            if ($found) {

                $JavaExe = $found.FullName

                $JavaBin = Split-Path $found.FullName -Parent

                $env:PATH = "$JavaBin;$env:PATH"

                $env:JAVA_HOME = Split-Path $JavaBin -Parent

                break

            }

        }

    }

}


if (-not $JavaExe) {

    Write-Error "Java not found. Install Java 8+ (64-bit) from https://adoptium.net"

    exit 1

}

$javaVersion = & { $ErrorActionPreference = 'Continue'; & $JavaExe -version 2>&1 } | Select-Object -First 1

Write-Host "  [OK] Java: $javaVersion"


# Docker

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {

    Write-Error "Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop"

    exit 1

}

$dockerInfo = & { $ErrorActionPreference = 'Continue'; & docker info 2>&1 }

if ($LASTEXITCODE -ne 0) {

    Write-Error "Docker is installed but not running. Start Docker Desktop and re-run."

    exit 1

}

Write-Host "  [OK] Docker is running"


# Python

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {

    Write-Error "Python not found. Install Python 3 from https://python.org"

    exit 1

}

$pyVersion = & python --version 2>&1

Write-Host "  [OK] $pyVersion"


# AWS CLI

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {

    Write-Error "AWS CLI not found. Install from https://aws.amazon.com/cli/ (no account needed -- download is public)"

    exit 1

}

$awsVersion = & { $ErrorActionPreference = 'Continue'; & aws --version 2>&1 } | Select-Object -First 1

Write-Host "  [OK] $awsVersion"


Write-Host ""


# ── Step 2: Build ─────────────────────────────────────────────────────────────


if ($env:SKIP_BUILD -eq "1") {

    Write-Host "Step 2: Skipping build (SKIP_BUILD=1)"

} else {

    Write-Host "Step 2: Building presto-sqlite connector..."


    $WrapperDir = Join-Path $ScriptDir ".mvn\wrapper"

    if (-not (Test-Path (Join-Path $WrapperDir "maven-wrapper.properties"))) {

        New-Item -ItemType Directory -Path $WrapperDir -Force | Out-Null

        @"

wrapperVersion=3.3.4

distributionType=only-script

distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip

"@ | Set-Content -Path (Join-Path $WrapperDir "maven-wrapper.properties") -NoNewline

    }


    $env:MAVEN_OPTS = "-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true -Djavax.net.ssl.trustStore=NUL -Djavax.net.ssl.trustStoreType=Windows-ROOT"


    Push-Location $ScriptDir

    try {

        & .\mvnw.cmd clean package -q

        if ($LASTEXITCODE -ne 0) { throw "Maven build failed." }

    } finally {

        Pop-Location

    }

    Write-Host "  Build complete."

}

Write-Host ""


# ── Step 3: Download PUDL database ────────────────────────────────────────────


$DataDir = Join-Path $ScriptDir "data"

$PudlZip = Join-Path $DataDir "pudl.sqlite.zip"

$PudlDb  = Join-Path $DataDir "pudl.sqlite"


if ($env:PUDL_DB) {

    $PudlDb = $env:PUDL_DB

    if (-not (Test-Path $PudlDb)) {

        Write-Error "PUDL_DB points to '$PudlDb' but the file does not exist."

        exit 1

    }

    Write-Host "Step 3: Using existing PUDL database at $PudlDb"

} elseif (Test-Path $PudlDb) {

    $dbSize = (Get-Item $PudlDb).Length / 1GB

    Write-Host "Step 3: PUDL database already exists at $PudlDb ($([math]::Round($dbSize, 1)) GB) -- skipping download"

} else {

    Write-Host "Step 3: Downloading PUDL database..."

    Write-Host "  Source: s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip"

    Write-Host "  This is ~3.7 GB compressed, ~21 GB uncompressed."

    Write-Host "  Download speed depends on your connection."

    Write-Host ""


    if (-not (Test-Path $DataDir)) {

        New-Item -ItemType Directory -Path $DataDir -Force | Out-Null

    }


    # Check disk space (need ~25 GB for zip + extracted)

    $drive = (Resolve-Path $DataDir).Drive

    $freeGB = [math]::Round((Get-PSDrive $drive.Name).Free / 1GB, 1)

    if ($freeGB -lt 25) {

        Write-Error "Not enough disk space. Need ~25 GB free, have $freeGB GB on drive $($drive.Name):."

        exit 1

    }

    Write-Host "  Disk space: $freeGB GB free (need ~25 GB)"


    Write-Host "  Downloading..."

    $dlStart = Get-Date

    & aws s3 cp --no-sign-request s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip $PudlZip

    if ($LASTEXITCODE -ne 0) {

        Write-Error "Download failed. Check your internet connection and AWS CLI installation."

        exit 1

    }

    $dlElapsed = [int]((Get-Date) - $dlStart).TotalSeconds

    $zipSize = [math]::Round((Get-Item $PudlZip).Length / 1GB, 1)

    Write-Host "  Downloaded $zipSize GB in ${dlElapsed}s"


    Write-Host "  Extracting (this may take a few minutes)..."

    $extractStart = Get-Date

    & tar -xf $PudlZip -C $DataDir

    if ($LASTEXITCODE -ne 0) {

        Write-Host "  tar extraction failed, trying PowerShell Expand-Archive..."

        Expand-Archive -Path $PudlZip -DestinationPath $DataDir -Force

    }

    $extractElapsed = [int]((Get-Date) - $extractStart).TotalSeconds

    Write-Host "  Extracted in ${extractElapsed}s"


    # Clean up zip to save disk space

    Remove-Item $PudlZip -Force

    Write-Host "  Removed zip file to save space."


    if (-not (Test-Path $PudlDb)) {

        # The zip might extract to a subdirectory or different filename

        $found = Get-ChildItem $DataDir -Filter "*.sqlite" -Recurse | Select-Object -First 1

        if ($found) {

            $PudlDb = $found.FullName

            Write-Host "  Database found at: $PudlDb"

        } else {

            $found = Get-ChildItem $DataDir -Filter "*.db" -Recurse | Select-Object -First 1

            if ($found) {

                $PudlDb = $found.FullName

                Write-Host "  Database found at: $PudlDb"

            } else {

                Write-Error "Could not find extracted database in $DataDir"

                exit 1

            }

        }

    }

}


$dbSizeGB = [math]::Round((Get-Item $PudlDb).Length / 1GB, 1)

Write-Host "  PUDL database: $PudlDb ($dbSizeGB GB)"

Write-Host ""


# ── Step 4: Python venv ───────────────────────────────────────────────────────


Write-Host "Step 4: Setting up Python environment..."


$VenvDir = Join-Path $ScriptDir ".venv"

$VenvPip = Join-Path $VenvDir "Scripts\pip.exe"

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"

$VenvValid = (Test-Path $VenvDir) -and (Test-Path $VenvPip)


if (-not $VenvValid) {

    if (Test-Path $VenvDir) {

        Remove-Item -Recurse -Force $VenvDir

    }

    python -m venv $VenvDir

    & { $ErrorActionPreference = 'Continue'; & $VenvPip install --upgrade pip -q 2>&1 | Out-Null }

    & { $ErrorActionPreference = 'Continue'; & $VenvPip install presto-python-client psycopg2-binary -q 2>&1 | Out-Null }

    Write-Host "  Python venv created."

} else {

    # Ensure dependencies are installed

    & { $ErrorActionPreference = 'Continue'; & $VenvPip install presto-python-client psycopg2-binary -q 2>&1 | Out-Null }

    Write-Host "  Python venv ready."

}

Write-Host ""


# ── Step 5: Catalog configs ───────────────────────────────────────────────────


Write-Host "Step 5: Writing catalog configurations..."


$CatalogDir = Join-Path $ScriptDir "etc\catalog"

if (-not (Test-Path $CatalogDir)) {

    New-Item -ItemType Directory -Path $CatalogDir -Force | Out-Null

}


$SqlitePropsPath = Join-Path $CatalogDir "sqlite.properties"

@"

connector.name=sqlite

sqlite.db=/data/sqlite.db

"@ | Set-Content -Path $SqlitePropsPath -NoNewline


$PostgresPropsPath = Join-Path $CatalogDir "postgres.properties"

@"

connector.name=postgresql

connection-url=jdbc:postgresql://presto-postgres:5432/demo

connection-user=presto

connection-password=presto

"@ | Set-Content -Path $PostgresPropsPath -NoNewline


Write-Host "  sqlite.properties   -> /data/sqlite.db"

Write-Host "  postgres.properties -> presto-postgres:5432/demo"

Write-Host ""


# ── Step 6: Start PostgreSQL ──────────────────────────────────────────────────


Write-Host "Step 6: Starting PostgreSQL..."


& { $ErrorActionPreference = 'Continue'; & docker network create presto-net 2>&1 | Out-Null }


# Check if container already exists and is running

$pgStatus = & { $ErrorActionPreference = 'Continue'; & docker inspect -f '{{.State.Running}}' presto-postgres 2>&1 }

if ($pgStatus -eq "true") {

    Write-Host "  PostgreSQL container already running."

} else {

    & { $ErrorActionPreference = 'Continue'; & docker rm -f presto-postgres 2>&1 | Out-Null }


    & docker run -d --name presto-postgres --network presto-net -e POSTGRES_DB=demo -e POSTGRES_USER=presto -e POSTGRES_PASSWORD=presto -p 5433:5432 postgres:16




    if ($LASTEXITCODE -ne 0) {

        Write-Error "Failed to start PostgreSQL container."

        exit 1

    }


    Write-Host "  Waiting for PostgreSQL to accept connections..."

    $pgReady = $false

    for ($i = 0; $i -lt 30; $i++) {

        Start-Sleep -Seconds 2

        $pgCheck = & { $ErrorActionPreference = 'Continue'; & docker exec presto-postgres pg_isready -U presto 2>&1 }

        if ($LASTEXITCODE -eq 0) { $pgReady = $true; break }

    }


    if (-not $pgReady) {

        Write-Error "PostgreSQL did not become ready. Check: docker logs presto-postgres"

        exit 1

    }

    Write-Host "  PostgreSQL is ready."

}


Write-Host "  Seeding PostgreSQL with PUDL reference data..."

& $VenvPython "$ScriptDir\demo\create_postgres_pudl.py"

Write-Host ""


# ── Step 7: Start Presto ──────────────────────────────────────────────────────


Write-Host "Step 7: Starting Presto $PRESTO_VERSION..."


$PluginSrc = "$ScriptDir\target\presto-sqlite-$PRESTO_VERSION\sqlite"


if (-not (Test-Path $PluginSrc)) {

    Write-Error "Plugin directory not found at $PluginSrc. Did the build succeed?"

    exit 1

}


# Check if Presto is already running and healthy

$prestoRunning = $false

try {

    $response = Invoke-WebRequest -Uri "http://localhost:8080/v1/info" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop

    if ($response.StatusCode -eq 200) {

        $prestoRunning = $true

    }

} catch { }


if ($prestoRunning) {

    Write-Host "  Presto is already running on port 8080."

    Write-Host "  Restarting with PUDL database..."

    & { $ErrorActionPreference = 'Continue'; & docker rm -f presto 2>&1 | Out-Null }

    Start-Sleep -Seconds 2

}

else {

    & { $ErrorActionPreference = 'Continue'; & docker rm -f presto 2>&1 | Out-Null }

}


$prestoStarted = $false
for ($attempt = 1; $attempt -le 3; $attempt++) {
    & docker run -d --name presto -p 8080:8080 --network presto-net `
        -v "${PluginSrc}:/opt/presto-server/plugin/sqlite" `
        -v "${SqlitePropsPath}:/opt/presto-server/etc/catalog/sqlite.properties" `
        -v "${PostgresPropsPath}:/opt/presto-server/etc/catalog/postgres.properties" `
        -v "${PudlDb}:/data/sqlite.db:ro" `
        "prestodb/presto:$PRESTO_VERSION"


    if ($LASTEXITCODE -eq 0) {

        $prestoStarted = $true

        break

    }


    if ($attempt -lt 3) {

        Write-Host "  Docker command failed (attempt $attempt/3). Retrying in 5 seconds..."

        Write-Host "  (If you just approved a file-sharing prompt, this is expected.)"

        & { $ErrorActionPreference = 'Continue'; & docker rm -f presto 2>&1 | Out-Null }

        Start-Sleep -Seconds 5

    }

}


if (-not $prestoStarted) {

    Write-Error "Failed to start Presto container after 3 attempts."

    exit 1

}


Write-Host "  Waiting for Presto to be ready..."

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


if (-not $ready) {

    Write-Error "Presto did not become ready after 3 minutes. Check: docker logs presto"

    exit 1

}

Write-Host "  Presto is running at http://localhost:8080"


# Brief pause to let catalogs initialize

Start-Sleep -Seconds 5


# Verify catalogs

Write-Host "  Verifying catalogs..."

$catalogCheck = & $VenvPython -c "

import prestodb, sys

try:

    conn = prestodb.dbapi.connect(host='localhost', port=8080, user='test')

    cur = conn.cursor()

    cur.execute('SHOW CATALOGS')

    catalogs = [r[0] for r in cur.fetchall()]

    print('Catalogs: ' + ', '.join(catalogs))

    if 'sqlite' not in catalogs:

        print('ERROR: sqlite catalog not found', file=sys.stderr)

        sys.exit(1)

    if 'postgres' not in catalogs:

        print('WARNING: postgres catalog not found')

    cur.close()

    conn.close()

except Exception as e:

    print(f'ERROR: {e}', file=sys.stderr)

    sys.exit(1)

" 2>&1

Write-Host "  $catalogCheck"

Write-Host ""


# ── Step 8: Run SQLite benchmark ──────────────────────────────────────────────


$BenchmarkRuns = if ($env:BENCHMARK_RUNS) { $env:BENCHMARK_RUNS } else { "3" }


if ($env:SKIP_BENCHMARK -eq "1") {

    Write-Host "Step 8: Skipping SQLite benchmark (SKIP_BENCHMARK=1)"

} else {

    Write-Host "Step 8: Running SQLite benchmark (PUDL database)..."

    Write-Host "  Tables: 19K to 3.3M rows | Runs: $BenchmarkRuns per query"

    Write-Host "  This will take several minutes."

    Write-Host ""

    Write-Host "------------------------------------------------------------"


    $env:PYTHONIOENCODING = "utf-8"

    & $VenvPython "$ScriptDir\demo\benchmark_pudl.py" --large --runs $BenchmarkRuns --warmup 1 --timeout 120


    Write-Host "------------------------------------------------------------"

}

Write-Host ""


# ── Step 9: Run cross-connector test ──────────────────────────────────────────


Write-Host "Step 9: Running cross-connector federation test (SQLite + PostgreSQL)..."

Write-Host ""

Write-Host "------------------------------------------------------------"


$env:PYTHONIOENCODING = "utf-8"

& $VenvPython "$ScriptDir\demo\cross_connector_test.py"


Write-Host "------------------------------------------------------------"

Write-Host ""


# ── Done ──────────────────────────────────────────────────────────────────────


Write-Host "============================================================"

Write-Host "  PUDL Benchmark Complete"

Write-Host "============================================================"

Write-Host ""

Write-Host "  PUDL database:     $PudlDb ($dbSizeGB GB)"

Write-Host "  Presto:            http://localhost:8080"

Write-Host "  PostgreSQL:        localhost:5432 (presto/presto)"

Write-Host ""

Write-Host "  Re-run benchmarks:"

Write-Host "    .\.venv\Scripts\python.exe demo\benchmark_pudl.py --large"

Write-Host "    .\.venv\Scripts\python.exe demo\cross_connector_test.py"

Write-Host ""

Write-Host "  For detailed analysis see: BENCHMARK_REPORT.md"

Write-Host ""

Write-Host "  To start/stop containers:"

Write-Host "    docker start presto-postgres presto"

Write-Host "    docker stop  presto presto-postgres"

Write-Host ""


