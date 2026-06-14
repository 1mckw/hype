@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo.
echo ========================================
echo  4H Consensus Follow Backtest
echo ========================================
echo.

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found.
    pause
    exit /b 1
)

if not exist "output\top300_accounts.csv" (
    echo [ERROR] output/top300_accounts.csv not found.
    echo         Run run_top_traders.bat first.
    pause
    exit /b 1
)

if not exist "output" mkdir "output"

echo Uses cached fills: output/fills_cache.json
echo.

python "%~dp0backtest_4h.py" --output-dir "%~dp0output" --days 7 --top 5 --capital 10000 --max-accounts 200 --workers 16 --liquid-top-n 50
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE%==0 (
    echo [OK] Backtest finished.
    echo   - output/backtest_4h_summary.txt
    echo   - output/backtest_4h_trades.csv
    echo   - output/backtest_4h_report.html
    echo.
    start "" "%~dp0output\backtest_4h_report.html"
) else (
    echo [FAILED] Exit code: %EXITCODE%
)

echo.
pause
exit /b %EXITCODE%
