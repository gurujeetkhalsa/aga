# SPDX-FileCopyrightText: 2010 Philip Waldron
# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later
param(
    [string]$OutputRoot
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
$AppSource = Join-Path $RepoRoot "bayrate-app"
$BayRateSource = Join-Path $RepoRoot "bayrate"

if (-not $OutputRoot) {
    $OutputRoot = Join-Path $RepoRoot "_deploy"
}

$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Target = Join-Path $OutputRoot "bayrate-app-$Timestamp"
New-Item -ItemType Directory -Path $Target -Force | Out-Null

robocopy $AppSource $Target /E /XD .python_packages __pycache__ /XF local.settings.json *.pyc *.log *.csv | Out-Null
if ($LASTEXITCODE -gt 7) {
    throw "robocopy failed while preparing bayrate-app deployment package. Exit code: $LASTEXITCODE"
}

$BayRateTarget = Join-Path $Target "bayrate"
New-Item -ItemType Directory -Path $BayRateTarget -Force | Out-Null

$AllowedBayRateFiles = @(
    "__init__.py",
    "auth.py",
    "commit_staged_run.py",
    "core.py",
    "report_parser.py",
    "replay_staged_run.py",
    "snapshot_refresh.py",
    "sql_adapter.py",
    "stage_reports.py"
)

foreach ($FileName in $AllowedBayRateFiles) {
    $SourcePath = Join-Path $BayRateSource $FileName
    if (-not (Test-Path $SourcePath)) {
        throw "Required BayRate file was not found: $SourcePath"
    }
    Copy-Item -Path $SourcePath -Destination $BayRateTarget -Force
}

foreach ($NoticeFileName in @("COPYING", "NOTICE.md")) {
    $NoticePath = Join-Path $BayRateSource $NoticeFileName
    if (Test-Path $NoticePath) {
        Copy-Item -Path $NoticePath -Destination $BayRateTarget -Force
    }
}

Write-Output $Target
