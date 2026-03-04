@echo off
REM ClickHouse ODBC DSN Creation Script
REM
REM Usage:
REM   create_dsn.bat                                        - Create DSN interactively
REM   create_dsn.bat NAME HOST PORT DB USER                 - Create with specified parameters
REM   create_dsn.bat MyClickHouse clickhouse.example.com 8123 default default
REM   create_dsn.bat MyClickHouse clickhouse.example.com 8443 default default /ssl
REM
REM Options:
REM   /system  - Create System DSN (requires administrator)
REM   /ssl     - Enable SSL/TLS (HTTPS)
REM
REM Registers a User DSN (HKCU).
REM For a System DSN (HKLM), run as administrator with /system option.

setlocal enabledelayedexpansion

set DSN_HIVE=HKCU
set SYSTEM_DSN=0
set CH_SSL=0
set CH_SSL_VERIFY=1

REM Check for /system and /ssl options
for %%a in (%*) do (
    if /I "%%a"=="/system" (
        set DSN_HIVE=HKLM
        set SYSTEM_DSN=1
    )
    if /I "%%a"=="/ssl" (
        set CH_SSL=1
    )
)

if "%~1"=="" goto :interactive
if "%~1"=="/system" goto :interactive

REM --- Argument mode (DSN name and host are required) ---
set DSN_NAME=%~1
set CH_HOST=%~2

if "%CH_HOST%"=="" (
    echo ERROR: Please specify the hostname.
    echo.
    echo Usage: %~nx0 NAME HOST [PORT] [DB] [USER]
    echo   Ex:  %~nx0 MyClickHouse clickhouse.example.com 8123 default default
    echo.
    echo To create interactively, run without arguments:
    echo   %~nx0
    echo.
    pause
    exit /b 1
)

set CH_PORT=%~3
set CH_DB=%~4
set CH_UID=%~5

if "%CH_PORT%"=="" set CH_PORT=8123
if "%CH_DB%"=="" set CH_DB=default
if "%CH_UID%"=="" set CH_UID=default

goto :register

REM --- Interactive mode ---
:interactive
echo ============================================
echo  ClickHouse ODBC DSN Creation Wizard
echo ============================================
echo.

set /p DSN_NAME="DSN Name [MyClickHouse]: "
if "%DSN_NAME%"=="" set DSN_NAME=MyClickHouse

:input_host
set CH_HOST=
set /p CH_HOST="Hostname (required): "
if "%CH_HOST%"=="" (
    echo   Please enter a hostname.
    goto :input_host
)

set /p CH_PORT="Port [8123]: "
if "%CH_PORT%"=="" set CH_PORT=8123

set /p CH_DB="Database [default]: "
if "%CH_DB%"=="" set CH_DB=default

set /p CH_UID="Username [default]: "
if "%CH_UID%"=="" set CH_UID=default

set /p CH_PWD="Password []: "

set /p CH_SSL_INPUT="Enable SSL (Y/N) [N]: "
if /I "%CH_SSL_INPUT%"=="Y" (
    set CH_SSL=1
    if "%CH_PORT%"=="8123" set CH_PORT=8443
    set /p CH_SSL_VERIFY_INPUT="Verify SSL certificate (Y/N) [Y]: "
    if /I "!CH_SSL_VERIFY_INPUT!"=="N" set CH_SSL_VERIFY=0
)

echo.
echo --- Settings ---
echo   DSN:      %DSN_NAME%
echo   Host:     %CH_HOST%
echo   Port:     %CH_PORT%
echo   DB:       %CH_DB%
echo   User:     %CH_UID%
if %CH_SSL%==1 (
    echo   SSL:      Enabled ^(HTTPS^)
    if %CH_SSL_VERIFY%==0 (
        echo   Verify:   Disabled
    ) else (
        echo   Verify:   Enabled
    )
)
if %SYSTEM_DSN%==1 (
    echo   Type:     System DSN
) else (
    echo   Type:     User DSN
)
echo.
set /p CONFIRM="Register with these settings? (Y/N) [Y]: "
if /I "%CONFIRM%"=="N" (
    echo Cancelled.
    pause
    exit /b 0
)

:register
echo.
echo Registering DSN "%DSN_NAME%"...

REM Get driver path
set DRIVER_PATH=
for /f "tokens=2*" %%a in ('reg query "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "Driver" 2^>nul') do set DRIVER_PATH=%%b

if "%DRIVER_PATH%"=="" (
    echo.
    echo ERROR: CH ODBC Alternative is not registered.
    echo Please run register_driver.bat first.
    echo.
    pause
    exit /b 1
)

reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "Driver" /t REG_SZ /d "%DRIVER_PATH%" /f >nul
reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "Host" /t REG_SZ /d "%CH_HOST%" /f >nul
reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "Port" /t REG_SZ /d "%CH_PORT%" /f >nul
reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "Database" /t REG_SZ /d "%CH_DB%" /f >nul
reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "UID" /t REG_SZ /d "%CH_UID%" /f >nul

if defined CH_PWD (
    reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "PWD" /t REG_SZ /d "%CH_PWD%" /f >nul
)

reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "SSL" /t REG_SZ /d "%CH_SSL%" /f >nul
reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v "SSL_Verify" /t REG_SZ /d "%CH_SSL_VERIFY%" /f >nul

reg add "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "%DSN_NAME%" /t REG_SZ /d "CH ODBC Alternative" /f >nul

echo.
echo DSN "%DSN_NAME%" has been registered.
echo.
echo Connection string:
if %CH_SSL%==1 (
    echo   DSN=%DSN_NAME%;UID=%CH_UID%;PWD=***;SSL=1;
) else (
    echo   DSN=%DSN_NAME%;UID=%CH_UID%;PWD=***;
)
echo.
echo You can verify in ODBC Data Source Administrator (odbcad32.exe).
echo.
pause
