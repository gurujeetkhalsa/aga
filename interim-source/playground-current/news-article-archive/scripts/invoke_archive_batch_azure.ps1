param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('Load', 'ValidateLinks', 'LoadAndValidateLinks', 'QueueFailedLinks')]
  [string]$Mode,

  [Parameter(Mandatory = $true)]
  [string]$SampleName,

  [string]$RunKey = $SampleName,
  [string]$SourceDescription = '',
  [double]$LinkTimeoutSeconds = 5,
  [string]$FunctionAppName = 'aga-clubexpress-mail',
  [string]$ResourceGroupName = 'aga-data-platform'
)

$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')
$sampleDir = Join-Path $repoRoot "news-article-archive-samples\$SampleName"
$reportDir = Join-Path $repoRoot 'news-article-archive\reports'
$reportPath = Join-Path $reportDir "$RunKey-validation.json"
$loader = Join-Path $repoRoot 'news-article-archive\scripts\load_sample_to_azure.py'

function Set-ArchiveAzureEnvironment {
  $settingsJson = (az functionapp config appsettings list --name $FunctionAppName --resource-group $ResourceGroupName --only-show-errors -o json) -join "`n"
  $settings = ConvertFrom-Json -InputObject $settingsJson
  $sql = $null
  $storage = $null
  foreach ($item in $settings) {
    if ($item.name -eq 'SQL_CONNECTION_STRING') { $sql = $item.value }
    if ($item.name -eq 'AzureWebJobsStorage') { $storage = $item.value }
  }
  if (-not $sql) { throw 'SQL_CONNECTION_STRING app setting was not found.' }
  if (-not $storage) { throw 'AzureWebJobsStorage app setting was not found.' }
  $env:SQL_CONNECTION_STRING = $sql
  $env:AZURE_STORAGE_CONNECTION_STRING = $storage
}

function Clear-ArchiveAzureEnvironment {
  Remove-Item Env:\SQL_CONNECTION_STRING -ErrorAction SilentlyContinue
  Remove-Item Env:\AZURE_STORAGE_CONNECTION_STRING -ErrorAction SilentlyContinue
}

function Invoke-ArchiveLoader {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$Arguments,
    [bool]$AllowValidationFailure = $false
  )

  & py -3 $loader @Arguments
  $exitCode = $LASTEXITCODE
  if ($exitCode -ne 0 -and -not $AllowValidationFailure) {
    throw "Archive loader exited with code $exitCode."
  }
  if ($exitCode -ne 0 -and $AllowValidationFailure -and -not (Test-Path $reportPath)) {
    throw "Archive loader exited with code $exitCode and did not write $reportPath."
  }
}

function Write-FailedLinkQueue {
  if (-not (Test-Path $reportPath)) {
    throw "Validation report does not exist: $reportPath"
  }

  $report = Get-Content -Raw $reportPath | ConvertFrom-Json
  $failed = @($report.external_links.links | Where-Object { -not $_.valid } | ForEach-Object {
    [pscustomobject]@{
      run_id = $report.run_id
      run_key = $report.run_key
      status = $_.status
      http_status = $_.http_status
      method = $_.method
      url = $_.url
      error = $_.error
      article_count = @($_.articles).Count
      articles = ((@($_.articles) | ForEach-Object { "$($_.item_id): $($_.headline)" }) -join ' | ')
      repair_notes = ''
      repaired = 'false'
    }
  })

  $csvPath = Join-Path $reportDir "$RunKey-failed-links.csv"
  $jsonPath = Join-Path $reportDir "$RunKey-failed-links.json"
  $failed | Export-Csv -NoTypeInformation -Encoding UTF8 $csvPath
  $failed | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $jsonPath

  [pscustomobject]@{
    run_key = $RunKey
    failed_links = @($failed).Count
    csv = $csvPath
    json = $jsonPath
  } | ConvertTo-Json -Depth 4
}

function Invoke-Load {
  $args = @(
    '--sample-dir', $sampleDir,
    '--run-key', $RunKey,
    '--resume'
  )
  if ($SourceDescription) {
    $args += @('--source-description', $SourceDescription)
  }
  Invoke-ArchiveLoader -Arguments $args
}

function Invoke-ValidateLinks {
  $args = @(
    '--sample-dir', $sampleDir,
    '--run-key', $RunKey,
    '--resume',
    '--validate-only',
    '--check-external-links',
    '--link-timeout-seconds', ([string]$LinkTimeoutSeconds)
  )
  Invoke-ArchiveLoader -Arguments $args -AllowValidationFailure $true
}

Set-Location $repoRoot
Set-ArchiveAzureEnvironment
try {
  if ($Mode -eq 'Load') {
    Invoke-Load
  } elseif ($Mode -eq 'ValidateLinks') {
    Invoke-ValidateLinks
    Write-FailedLinkQueue
  } elseif ($Mode -eq 'QueueFailedLinks') {
    Write-FailedLinkQueue
  } elseif ($Mode -eq 'LoadAndValidateLinks') {
    Invoke-Load
    Invoke-ValidateLinks
    Write-FailedLinkQueue
  }
} finally {
  Clear-ArchiveAzureEnvironment
}
