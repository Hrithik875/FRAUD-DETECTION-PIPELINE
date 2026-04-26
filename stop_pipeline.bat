@echo off
chcp 65001 >nul
title Stopping Pipeline

set "BASE=%~dp0"
if "%BASE:~-1%"=="\" set "BASE=%BASE:~0,-1%"

echo ============================================================
echo   STOPPING FRAUD DETECTION PIPELINE
echo ============================================================
echo.

:: Stop containers
echo Stopping Docker containers...
cd /d "%BASE%\docker"
docker compose down
echo [OK] Containers stopped.

:: Clean up temp spark bat if exists
if exist "%BASE%\run_spark.bat" del "%BASE%\run_spark.bat"
echo [OK] Cleanup done.

echo.
echo   Note: Close Producer, Spark, Flask windows manually.
echo ============================================================
pause