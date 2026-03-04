@echo off
REM ============================================
REM  ClickHouse ODBC Driver - Release Package
REM ============================================
REM
REM Collects the Release-built DLL and setup files
REM into the dist\ folder and creates a ZIP.
REM
REM Usage:
REM   build_release_package.bat              - Use default version (1.1.0)
REM   build_release_package.bat 1.2.0        - Specify version

setlocal

if "%~1"=="" (
    set VERSION=1.1.0
) else (
    set VERSION=%~1
)

set DIST_DIR=dist\ch-odbc-alternative-%VERSION%-x64
set DLL_PATH=build\Release\ch-odbc-alternative.dll

echo ============================================
echo  Creating Release Package
echo ============================================
echo.

REM Check for Release DLL
if not exist "%DLL_PATH%" (
    echo ERROR: Release build not found: %DLL_PATH%
    echo Please build with the Release configuration first.
    echo.
    pause
    exit /b 1
)

REM Clean up existing dist folder
if exist "%DIST_DIR%" (
    echo Removing existing dist folder...
    rmdir /s /q "%DIST_DIR%"
)
mkdir "%DIST_DIR%"

echo Copying files from build\Release\ ...
echo.

REM All files are in build\Release\ (DLL + scripts copied by CopySetupFiles target)
for %%F in (
    ch-odbc-alternative.dll
    ch-odbc-alternative-installer.exe
    register_driver.bat
    unregister_driver.bat
    create_dsn.bat
    remove_dsn.bat
    setup.inf
    README_SETUP.txt
) do (
    if exist "build\Release\%%F" (
        copy /Y "build\Release\%%F" "%DIST_DIR%\" >nul
        echo   [OK] %%F
    ) else (
        echo   [WARN] %%F not found
    )
)

echo.
echo ============================================
echo  Package created successfully
echo ============================================
echo.
echo Output: %DIST_DIR%\
echo.

dir /B "%DIST_DIR%"

echo.

REM Create ZIP
set ZIP_NAME=ch-odbc-alternative-%VERSION%-x64.zip
echo Creating %ZIP_NAME% ...
powershell -NoProfile -Command "Compress-Archive -Path '%DIST_DIR%\*' -DestinationPath 'dist\%ZIP_NAME%' -Force"
if exist "dist\%ZIP_NAME%" (
    echo   [OK] dist\%ZIP_NAME%
) else (
    echo   [WARN] ZIP creation failed
)

echo.
echo Done. Upload dist\%ZIP_NAME% to GitHub Releases.
echo.
pause
