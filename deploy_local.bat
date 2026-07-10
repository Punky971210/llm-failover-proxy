@echo off
chcp 65001 >nul
title LLM Failover Proxy - Local Deploy
setlocal enabledelayedexpansion

echo ====================================================
echo  LLM Failover Proxy - Local Deploy Script
echo  Admin Panel: http://localhost:8000/admin
echo  No password (local access mode)
echo ====================================================
echo.

:: config.yaml 本地模式已关闭认证，无需 override

:: Stop and remove old container
echo [1/5] Stopping old container...
docker compose down >nul 2>&1

:: Remove old image to force a clean build (avoids stale layer cache)
echo [2/5] Removing old image...
docker rmi -f proxy-llm-failover:latest >nul 2>&1

:: Clean Docker build cache for proxy image only
echo [3/5] Cleaning build cache...
docker builder prune --filter type=exec.cachemount --force >nul 2>&1

:: Build with cache buster to ensure fresh COPY
echo [4/5] Building Docker image...
for /f %%i in ('powershell -Command "Get-Date -UFormat %%s"') do set APP_SRC_HASH=%%i
docker compose build --build-arg APP_SRC_HASH=%APP_SRC_HASH%
if errorlevel 1 (
    echo.
    echo [ERROR] Build failed. Check Docker is running and try again.
    pause
    exit /b 1
)

:: Start container
echo [5/5] Starting container...
docker compose up -d
if errorlevel 1 (
    echo.
    echo [ERROR] Failed to start container.
    pause
    exit /b 1
)

:: Wait for health check
echo Waiting for service to be ready...
:wait_loop
timeout /t 3 /nobreak >nul
docker exec llm-failover-proxy python -c "import httpx; httpx.get('http://localhost:8000/health',timeout=5).raise_for_status()" >nul 2>&1
if errorlevel 1 goto wait_loop

echo.
echo ====================================================
echo   Service is ready!
echo.
echo   Admin Panel: http://localhost:8000/admin
echo   Health:      http://localhost:8000/health
echo   Models:      http://localhost:8000/v1/models
echo ====================================================
echo.

:: Open browser
start http://localhost:8000/admin
