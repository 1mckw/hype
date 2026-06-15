@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo.
echo ========================================
echo  Hyperliquid Consensus Telegram Bot
echo ========================================
echo.

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found.
    pause
    exit /b 1
)

if "%TELEGRAM_BOT_TOKEN%"=="" (
    echo [ERROR] Set TELEGRAM_BOT_TOKEN environment variable.
    echo         Example: set TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
    pause
    exit /b 1
)
if "%TELEGRAM_CHAT_ID%"=="" (
    echo [ERROR] Set TELEGRAM_CHAT_ID environment variable.
    echo         Message your bot, then visit:
    echo         https://api.telegram.org/bot^<TOKEN^>/getUpdates
    pause
    exit /b 1
)

if not exist "output" mkdir "output"

echo Schedule: poll every 1 hour for new trades (GitHub Actions)
echo Test once: python telegram_bot.py --once
echo Reset baseline: python telegram_bot.py --bootstrap
echo.

python "%~dp0telegram_bot.py" --loop --interval-min 60 --output-dir "%~dp0output"
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE%==0 (
    echo [OK] Bot stopped.
) else (
    echo [FAILED] Exit code: %EXITCODE%
)
pause
exit /b %EXITCODE%
