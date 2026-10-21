# Use the workspace-local tools installed during this session, when available.
$projectRoot = Split-Path -Parent $PSScriptRoot
$localGoRoot = Join-Path $projectRoot '.tools/toolchain/go'
if (Test-Path -LiteralPath (Join-Path $localGoRoot 'bin/go.exe')) {
    $env:GOROOT = $localGoRoot
    $env:GOPATH = Join-Path $projectRoot '.tools/gopath'
    $env:GOCACHE = Join-Path $projectRoot '.tools/gocache'
    $env:PATH = "$localGoRoot/bin;$(Join-Path $projectRoot '.tools/bin');$(Join-Path $projectRoot 'bin');$env:PATH"
}
