@echo off
set SERVER=10.10.0.113
set DRIVE_LETTER=Z:
set USERNAME=admin
set PASSWORD=adminkepri

echo === Network Drive Connection ===

echo [1/4] Cleaning up any existing connections...
net use %DRIVE_LETTER% /delete /y >nul 2>&1
net use \\%SERVER% /delete /y >nul 2>&1

echo [2/4] Testing connectivity to %SERVER%...
ping -n 1 %SERVER% >nul
if %errorlevel% neq 0 (
    echo ERROR: Cannot ping %SERVER%
    pause
    exit /b 1
)

echo Username: %USERNAME%
echo Password: %PASSWORD%
echo [3/4]Connecting with credentials...
net use %DRIVE_LETTER% "\\%SERVER%\Produksi TVRI" "%PASSWORD%" /user:"%USERNAME%" /persistent:no

if %errorlevel% == 0 (
    echo Successfully connected to %DRIVE_LETTER% with credentials
    goto RUNSCRIPT
) else (
    echo Failed to connect. Error code: %errorlevel%
    pause
    exit /b 1
)

:RUNSCRIPT
echo [4/4] Running Python script...
cd /d "C:\CPNS\app\File Watcher py"
pythonw PCRecord.py
 