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

echo Schedule: 4H TOP5 every 4H UTC, 24H TOP5 at 00:00 UTC
echo Optional: add --boot-notify to push immediately on start
echo Test once: python telegram_bot.py --once
echo.

python "%~dp0telegram_bot.py" --loop --boot-notify --output-dir "%~dp0output"
set EXITCODE=%ERRORLEVEL%

echo.
if %EXITCODE%==0 (
    echo [OK] Bot stopped.
) else (
    echo [FAILED] Exit code: %EXITCODE%
)
pause
exit /b %EXITCODE%
