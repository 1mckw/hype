@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo.
echo ========================================
echo  Top Trader Scanner - Hyperliquid
echo ========================================
echo.

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Install Python 3.8+ and add to PATH.
    echo         https://www.python.org/downloads/
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo Using Python %PYVER%
echo.

if not exist "output" mkdir "output"

echo Fast mode: active accounts, 30D PnL ^> 0, 1Y ROI ^> 150%%, 24 workers
echo.

python "%~dp0fetch_top_traders.py" --fast --workers 24 --count 300 --min-year-roi 1.5 --min-closed 5 --scan-limit 2000 --output-dir "%~dp0output"
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE%==0 (
    echo [OK] Finished. Output files:
    echo   - output/top300_accounts.csv
    echo   - output/consensus_24h.csv and consensus_4h.csv
    echo   - output/trades_4h.csv and trades_24h.csv
    echo   - output/summary.txt
    echo   - output/report.html
    echo.
    echo Opening HTML report...
    start "" "%~dp0output\report.html"
) else (
    echo [FAILED] Exit code: %EXITCODE%
)

echo.
pause
exit /b %EXITCODE%
