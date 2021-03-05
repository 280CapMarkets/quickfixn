echo off

set CONFIGURATION=%1

if "%1" == "" set CONFIGURATION=Release

REM pwsh is in PowerShell 6+
pwsh -v >nul 2>&1
if "%ERRORLEVEL%" == "0" (
	pwsh .\build.ps1 %CONFIGURATION%
) else (
	powershell .\build.ps1 %CONFIGURATION%
)

dotnet pack QuickFIXn\QuickFix.csproj
dotnet pack Messages\FIX40\QuickFix.FIX40.csproj
dotnet pack Messages\FIX41\QuickFix.FIX41.csproj
dotnet pack Messages\FIX42\QuickFix.FIX42.csproj
dotnet pack Messages\FIX43\QuickFix.FIX43.csproj
dotnet pack Messages\FIX44\QuickFix.FIX44.csproj
dotnet pack Messages\FIX50\QuickFix.FIX50.csproj
dotnet pack Messages\FIX50SP1\QuickFix.FIX50SP1.csproj
dotnet pack Messages\FIX50SP2\QuickFix.FIX50SP2.csproj
