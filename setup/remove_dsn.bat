@echo off
REM ClickHouse ODBC DSN Removal Script
REM
REM Usage:
REM   remove_dsn.bat DSN_NAME              - Remove a User DSN
REM   remove_dsn.bat DSN_NAME /system      - Remove a System DSN (requires admin)
REM   remove_dsn.bat                       - List registered DSNs

setlocal enabledelayedexpansion

if "%~1"=="" goto :list_dsn

set DSN_NAME=%~1
set DSN_HIVE=HKCU

if /I "%~2"=="/system" set DSN_HIVE=HKLM

echo Removing DSN "%DSN_NAME%"...
echo.

reg delete "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /f 2>nul
if %ERRORLEVEL% EQU 0 (
    echo   [OK] DSN settings removed
) else (
    echo   [--] DSN "%DSN_NAME%" not found
)

reg delete "%DSN_HIVE%\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "%DSN_NAME%" /f 2>nul
if %ERRORLEVEL% EQU 0 (
    echo   [OK] Removed from data source list
) else (
    echo   [--] Not found in data source list
)

echo.
echo Done.
echo.
pause
exit /b 0

:list_dsn
echo ============================================
echo  ClickHouse ODBC DSN List
echo ============================================
echo.

echo --- User DSN (HKCU) ---
set FOUND=0
for /f "tokens=1,2*" %%a in ('reg query "HKCU\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" 2^>nul ^| findstr /I "ClickHouse"') do (
    echo   %%a
    set FOUND=1
)
if %FOUND%==0 echo   (none)

echo.
echo --- System DSN (HKLM) ---
set FOUND=0
for /f "tokens=1,2*" %%a in ('reg query "HKLM\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" 2^>nul ^| findstr /I "ClickHouse"') do (
    echo   %%a
    set FOUND=1
)
if %FOUND%==0 echo   (none)

echo.
echo To remove: %~nx0 DSN_NAME
echo.
pause
