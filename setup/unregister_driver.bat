@echo off
REM CH ODBC Alternative Unregistration Script
REM Automatically requests administrator privileges via UAC.
REM
REM Usage:
REM   unregister_driver.bat            - Remove driver registration
REM   unregister_driver.bat DSN_NAME   - Also remove specified DSN

REM --- UAC self-elevation ---
net session >nul 2>&1
if %ERRORLEVEL% neq 0 (
    powershell -Command "Start-Process -FilePath '%~f0' -ArgumentList '%*' -Verb RunAs -Wait"
    exit /b
)

echo Removing ClickHouse ODBC driver registration...
echo.

REM Remove driver registration
reg delete "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /f 2>nul
if %ERRORLEVEL% EQU 0 (
    echo   [OK] Driver settings removed
) else (
    echo   [--] Driver settings not found (already removed)
)

reg delete "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "CH ODBC Alternative" /f 2>nul
if %ERRORLEVEL% EQU 0 (
    echo   [OK] Removed from driver list
) else (
    echo   [--] Not found in driver list
)

REM Remove DSN if a name was specified
if not "%~1"=="" (
    echo.
    echo Removing DSN "%~1"...
    reg delete "HKCU\SOFTWARE\ODBC\ODBC.INI\%~1" /f 2>nul
    if %ERRORLEVEL% EQU 0 (
        echo   [OK] DSN settings removed (HKCU)
    ) else (
        echo   [--] User DSN "%~1" not found
    )
    reg delete "HKCU\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "%~1" /f 2>nul

    REM Check System DSN
    reg delete "HKLM\SOFTWARE\ODBC\ODBC.INI\%~1" /f 2>nul
    if %ERRORLEVEL% EQU 0 (
        echo   [OK] System DSN settings removed (HKLM)
    )
    reg delete "HKLM\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "%~1" /f 2>nul
)

echo.
echo Unregistration complete.
echo.
pause
