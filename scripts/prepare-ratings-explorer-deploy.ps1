param(
    [string]$OutputRoot
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$AppSource = Join-Path $RepoRoot "ratings-explorer-app"
$BayRateSource = Join-Path $RepoRoot "bayrate"

if (-not $OutputRoot) {
    $OutputRoot = Join-Path $RepoRoot "_deploy"
}

$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Target = Join-Path $OutputRoot "ratings-explorer-app-bayrate-$Timestamp"
New-Item -ItemType Directory -Path $Target -Force | Out-Null

robocopy $AppSource $Target /E /XD .python_packages __pycache__ data /XF local.settings.json *.pyc *.log *.csv | Out-Null
if ($LASTEXITCODE -gt 7) {
    throw "robocopy failed while preparing ratings-explorer-app deployment package. Exit code: $LASTEXITCODE"
}

$BayRateTarget = Join-Path $Target "bayrate"
New-Item -ItemType Directory -Path $BayRateTarget -Force | Out-Null
Copy-Item -Path (Join-Path $BayRateSource "*.py") -Destination $BayRateTarget -Force
Copy-Item -Path (Join-Path $BayRateSource "COPYING") -Destination $BayRateTarget -Force
Copy-Item -Path (Join-Path $BayRateSource "README.md") -Destination $BayRateTarget -Force

Write-Output $Target
