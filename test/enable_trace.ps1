# ClickHouse ODBC Driver Trace Log Enablement Script
# Usage:
#   .\enable_trace.ps1                    # Start tracing with default log file
#   .\enable_trace.ps1 -LogFile "C:\temp\odbc_trace.log"  # Output log to specified path
#   .\enable_trace.ps1 -Disable           # Disable tracing
#
# Testing procedure with MS Access:
#   1. Run this script to enable tracing
#   2. Launch MS Access and perform table linking
#   3. Check the log file
#   4. Disable with -Disable

param(
    [string]$LogFile = "",
    [switch]$Disable
)

if ($Disable) {
    [System.Environment]::SetEnvironmentVariable("CLICKHOUSE_ODBC_TRACE", $null, "User")
    Write-Host "CLICKHOUSE_ODBC_TRACE has been disabled." -ForegroundColor Yellow
    Write-Host "Please restart MS Access for the change to take effect." -ForegroundColor Yellow
    return
}

if ([string]::IsNullOrEmpty($LogFile)) {
    $LogFile = Join-Path $PSScriptRoot "clickhouse_odbc_trace.log"
}

[System.Environment]::SetEnvironmentVariable("CLICKHOUSE_ODBC_TRACE", $LogFile, "User")
Write-Host "CLICKHOUSE_ODBC_TRACE has been enabled." -ForegroundColor Green
Write-Host "Log file: $LogFile" -ForegroundColor Green
Write-Host ""
Write-Host "Steps:" -ForegroundColor Cyan
Write-Host "  1. Launch MS Access (a new process is required for the env variable to take effect)" -ForegroundColor Cyan
Write-Host "  2. Perform table linking operation" -ForegroundColor Cyan
Write-Host "  3. Check the log file: $LogFile" -ForegroundColor Cyan
Write-Host "  4. When done: .\enable_trace.ps1 -Disable" -ForegroundColor Cyan
Write-Host ""

# Check if an existing log file is present
if (Test-Path $LogFile) {
    $size = (Get-Item $LogFile).Length
    Write-Host "An existing log file was found ($size bytes)." -ForegroundColor Yellow
    Write-Host "New log entries will be appended to the end." -ForegroundColor Yellow
}
