@echo off
REM CH ODBC Alternative Registration Script
REM Automatically requests administrator privileges via UAC.
REM
REM Usage:
REM   register_driver.bat                    - Register using DLL in the script directory
REM   register_driver.bat C:\path\to\dll     - Register using DLL at specified path

REM --- UAC self-elevation ---
net session >nul 2>&1
if %ERRORLEVEL% neq 0 (
    powershell -Command "Start-Process -FilePath '%~f0' -ArgumentList '%*' -Verb RunAs -Wait"
    exit /b
)

if "%~1"=="" (
    set DRIVER_PATH=%~dp0ch-odbc-alternative.dll
) else (
    set DRIVER_PATH=%~1
)

if not exist "%DRIVER_PATH%" (
    echo ERROR: DLL not found: %DRIVER_PATH%
    echo.
    echo Usage: %~nx0 [path\to\ch-odbc-alternative.dll]
    pause
    exit /b 1
)

echo Registering CH ODBC Alternative...

REM Add driver to ODBC registry (64-bit)
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "CH ODBC Alternative" /t REG_SZ /d "Installed" /f >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to write to registry.
    pause
    exit /b 1
)
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "Driver" /t REG_SZ /d "%DRIVER_PATH%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "Setup" /t REG_SZ /d "%DRIVER_PATH%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "APILevel" /t REG_SZ /d "1" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "ConnectFunctions" /t REG_SZ /d "YYY" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "DriverODBCVer" /t REG_SZ /d "03.80" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "FileUsage" /t REG_SZ /d "0" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative" /v "SQLLevel" /t REG_SZ /d "1" /f >nul

REM Verify registration
reg query "HKLM\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" /v "CH ODBC Alternative" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Registration failed. Driver not found in registry.
    pause
    exit /b 1
)

echo.
echo Driver registered successfully.
echo.
echo To create a DSN, run create_dsn.bat or use ODBC Data Source Administrator (odbcad32.exe).
echo.
echo   create_dsn.bat MyClickHouse your-server.example.com 8123 default default
echo.
echo Or add a DSN via registry (replace host/port/database/user as needed):
echo.
echo   reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Driver" /t REG_SZ /d "%DRIVER_PATH%" /f
echo   reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Host" /t REG_SZ /d "your-server.example.com" /f
echo   reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Port" /t REG_SZ /d "8123" /f
echo   reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Database" /t REG_SZ /d "default" /f
echo   reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "UID" /t REG_SZ /d "default" /f
echo   reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "MyClickHouse" /t REG_SZ /d "CH ODBC Alternative" /f

pause
