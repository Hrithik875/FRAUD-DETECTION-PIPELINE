@echo off
chcp 65001 >nul
title Fraud Detection Pipeline

:: ── Set base path ────────────────────────────────────────────────────
set "BASE=%~dp0"
if "%BASE:~-1%"=="\" set "BASE=%BASE:~0,-1%"

echo ============================================================
echo   REAL-TIME FRAUD DETECTION PIPELINE
echo   BDA AAT - Big Data Analytics
echo ============================================================
echo.
echo [INFO] Base: %BASE%
echo.

:: ── Step 1: Check Docker ─────────────────────────────────────────────
echo [1/7] Checking Docker...
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker Desktop is not running.
    echo         Please start Docker Desktop first.
    pause
    exit /b 1
)
echo [OK] Docker is running.
echo.

:: ── Step 2: Start Containers ─────────────────────────────────────────
echo [2/7] Starting containers...
cd /d "%BASE%\docker"
docker compose up -d
if %errorlevel% neq 0 (
    echo [ERROR] Failed to start containers.
    pause
    exit /b 1
)
echo [OK] Containers started.
echo.

:: ── Step 3: Wait for Kafka ───────────────────────────────────────────
echo [3/7] Waiting for Kafka (max 60 seconds)...
set RETRY=0
:KAFKA_WAIT
set /a RETRY+=1
if %RETRY% gtr 20 (
    echo [ERROR] Kafka timeout. Check: docker logs kafka
    pause
    exit /b 1
)
docker exec kafka kafka-topics --list ^
    --bootstrap-server localhost:9092 >nul 2>&1
if %errorlevel% equ 0 goto KAFKA_OK
echo         Attempt %RETRY%/20...
timeout /t 3 /nobreak >nul
goto KAFKA_WAIT
:KAFKA_OK
echo [OK] Kafka ready.
echo.

:: ── Step 4: Wait for PostgreSQL ──────────────────────────────────────
echo [4/7] Waiting for PostgreSQL...
set RETRY=0
:PG_WAIT
set /a RETRY+=1
if %RETRY% gtr 15 (
    echo [ERROR] PostgreSQL timeout. Check: docker logs postgres_fraud
    pause
    exit /b 1
)
docker exec postgres_fraud pg_isready -U frauduser >nul 2>&1
if %errorlevel% equ 0 goto PG_OK
echo         Attempt %RETRY%/15...
timeout /t 2 /nobreak >nul
goto PG_WAIT
:PG_OK
echo [OK] PostgreSQL ready.
echo.

:: ── Step 4.5: Clear old DB data ──────────────────────────────────────
echo Clearing old transaction data...
docker exec postgres_fraud psql -U frauduser -d frauddb ^
    -c "TRUNCATE TABLE fraud_transactions RESTART IDENTITY; TRUNCATE TABLE merchant_stats RESTART IDENTITY;" >nul 2>&1
echo [OK] Database cleared for fresh run.
echo.

:: ── Step 5: Start Producer ───────────────────────────────────────────
echo [5/7] Starting Producer...
start "Fraud Producer" cmd /k ^
    "chcp 65001 >nul && cd /d "%BASE%\producer" && python producer.py"
timeout /t 3 /nobreak >nul
echo [OK] Producer started.
echo.

:: ── Step 6: Clear checkpoints + Start Spark ──────────────────────────
echo [6/7] Clearing checkpoints...
set "CKPT=%BASE%\data_output\checkpoints"

if exist "%CKPT%\transactions" rmdir /s /q "%CKPT%\transactions"
if exist "%CKPT%\merchants"    rmdir /s /q "%CKPT%\merchants"

mkdir "%CKPT%\transactions"
mkdir "%CKPT%\merchants"
echo [OK] Checkpoints cleared and recreated.
echo.

echo Starting Spark...
set "SPARK_CMD=%BASE%\run_spark.bat"

echo @echo off                                          > "%SPARK_CMD%"
echo chcp 65001 ^>nul                                  >> "%SPARK_CMD%"
echo cd /d "%BASE%\streaming"                          >> "%SPARK_CMD%"
echo spark-submit ^
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0 ^
spark_stream.py                                        >> "%SPARK_CMD%"

start "Spark Streaming" cmd /k ""%SPARK_CMD%""
echo [OK] Spark started (takes 30-60 seconds to initialize).
echo.

:: ── Step 7: Wait for Spark then start Flask ──────────────────────────
echo [7/7] Waiting 40 seconds for Spark to initialize...
timeout /t 40 /nobreak >nul

echo Starting Flask API...
start "Flask API" cmd /k ^
    "chcp 65001 >nul && cd /d "%BASE%\api" && python app.py"
timeout /t 3 /nobreak >nul
echo [OK] Flask API started.
echo.

:: ── Open Dashboard ───────────────────────────────────────────────────
echo Opening dashboard in browser...
timeout /t 2 /nobreak >nul
start "" "http://localhost:5000"

echo.
echo ============================================================
echo   PIPELINE RUNNING
echo ============================================================
echo   Dashboard  : http://localhost:5000
echo   Producer   : See "Fraud Producer" window
echo   Spark      : See "Spark Streaming" window
echo   Flask      : See "Flask API" window
echo.
echo   Checkpoints : auto-cleared on each startup
echo   Database    : auto-cleared on each startup
echo   To stop     : run stop_pipeline.bat
echo ============================================================
echo.
pause