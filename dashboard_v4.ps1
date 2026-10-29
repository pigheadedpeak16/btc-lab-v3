param([switch]$NoOpen,[int]$Port=8592)
$ErrorActionPreference='Stop'
$btcDashboardRoot=$PSScriptRoot
$btcDashboardPython=Join-Path $btcDashboardRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $btcDashboardPython)) { throw 'Project Python environment is missing.' }
Write-Host 'BTC LAB V4 - READ ONLY / PAPER ONLY' -ForegroundColor Cyan
if ($NoOpen) {
    & $btcDashboardPython -B -m btc_v3.forensics.browser_dashboard --root $btcDashboardRoot --port $Port
} else {
    & $btcDashboardPython -B -m btc_v3.forensics.browser_dashboard --root $btcDashboardRoot --port $Port --open
}
if ($LASTEXITCODE -ne 0) { throw ('Dashboard exited: '+$LASTEXITCODE) }
