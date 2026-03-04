========================================================
 CH ODBC Alternative v1.1.0
========================================================

** Included Files

  ch-odbc-alternative.dll            ... ODBC Driver
  ch-odbc-alternative-installer.exe  ... GUI Installer (optional)
  register_driver.bat                ... Driver registration script
  unregister_driver.bat              ... Driver unregistration script
  create_dsn.bat                     ... DSN creation wizard
  remove_dsn.bat                     ... DSN removal tool

** Requirements

  - Windows 10/11 (x64)
  - ClickHouse Server (HTTP interface, port 8123 or HTTPS port 8443)
  - OpenSSL (required only when SSL/TLS is enabled)

** Installation

  Method A: GUI Installer
    1. Run ch-odbc-alternative-installer.exe (UAC prompt will appear)
    2. Specify install directory, optionally create a DSN
    3. Click "Install"

    NOTE: If SmartScreen blocks the exe, use Method B.

  Method B: Batch Files
    1. Copy this folder to e.g. %LOCALAPPDATA%\CH ODBC Alternative\
    2. Run register_driver.bat (UAC prompt will appear)
    3. (Optional) Run create_dsn.bat:
         create_dsn.bat
         create_dsn.bat MyClickHouse clickhouse.example.com 8123 default default
         create_dsn.bat MyClickHouse clickhouse.example.com 8443 default default /ssl

** Connection String

  Driver={CH ODBC Alternative};Host=localhost;Port=8123;Database=default;UID=default;PWD=;

  Parameters:
    Host        ClickHouse server hostname or IP (required)
    Port        HTTP 8123 / HTTPS 8443 (default: 8123)
    Database    Database name (default: default)
    UID         Username (default: default)
    PWD         Password (default: empty)
    SSL         Enable SSL/TLS: 1, true, yes (default: 0)
    SSL_VERIFY  Verify server certificate: 0 to disable (default: 1)

  Advanced Parameters:
    LazyPaging    Enable lazy paging: 0 to disable (default: 1)
    PageSize      Rows per lazy-paging page (default: 10000)
    MaxLazyRows   Max rows in lazy paging mode (default: 0=unlimited)
    DefaultMaxRows  Default SQL_ATTR_MAX_ROWS for statements (default: 0=unlimited)

  Example (SSL):
    Driver={CH ODBC Alternative};Host=ch.example.com;SSL=1;Database=default;UID=default

** Using with MS Access

  1. Open Access -> "External Data" -> "ODBC Database"
  2. Select "Link to the data source by creating a linked table"
  3. Select your DSN or enter a connection string
  4. Select the tables to link

** Uninstallation

  GUI Installer:
    Run ch-odbc-alternative-installer.exe as administrator -> "Uninstall"

  Batch Files:
    1. remove_dsn.bat MyClickHouse
    2. unregister_driver.bat (as administrator)
    3. Delete this folder

** Troubleshooting

  Q: Driver does not appear in odbcad32.exe
  A: Run register_driver.bat as administrator.
     Use 64-bit odbcad32: C:\Windows\System32\odbcad32.exe

  Q: Cannot connect
  A: Verify ClickHouse is running.
     Access http://<hostname>:8123/ in a browser ("Ok." = working).

  Q: Cannot connect with SSL
  A: Verify HTTPS is enabled on ClickHouse (port 8443).
     For self-signed certificates, set SSL_VERIFY=0.

  Q: Tables do not appear in MS Access
  A: Verify the Database parameter is correct (default: "default").

  Q: "Query result size exceeds 2GB" error in MS Access
  A: This occurs when navigating to the last row of a large table.
     MS Access caches all rows locally and has a 2GB limit.
     Set MaxLazyRows in DSN settings (e.g., MaxLazyRows=500000).
     For wide tables, try MaxLazyRows=100000.

========================================================
