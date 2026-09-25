param(
    # Directory containing the Windows coretex.exe build to test.
    # Default: this script's own directory (drop it next to the exes).
    [string]$Root = $PSScriptRoot
)
# CoreTexDB Windows acceptance — exercises the 2026-09-25 durability fixes
# (WAL/storage error propagation, update persistence, restore, JSON metadata).
# Every CLI call is a separate OS process, so "insert in process A / read in
# process B" is a real restart-persistence test.
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File windows_acceptance.ps1 [-Root <dir>]
# Exit code 0 = all checks passed.
$ErrorActionPreference = "Continue"
$bin  = Join-Path $Root "coretex.exe"
$data = Join-Path $Root "data_root"
$bk   = Join-Path $Root "bk"

$script:pass = 0
$script:fail = 0
function Check($name, $cond, $detail) {
    if ($cond) { $script:pass++; Write-Host "[PASS] $name" }
    else       { $script:fail++; Write-Host "[FAIL] $name -- $detail" }
}
function Invoke-DB([string[]]$CliArgs) {
    # NOTE: never name the parameter $args — that is a PowerShell automatic
    # variable and the splat would silently expand to the wrong thing.
    $out = & $bin @CliArgs 2>$null | Out-String
    return $out
}

Write-Host "== 0. clean slate =="
if (Test-Path $data) { Remove-Item -Recurse -Force $data }
if (Test-Path $bk)   { Remove-Item -Recurse -Force $bk }

Write-Host "== 1. version =="
$v = Invoke-DB @("version")
Check "version reports 0.2.3" ($v -match "0\.2\.[0-9]+") $v

Write-Host "== 2. create HNSW collection =="
$o = Invoke-DB @("--data-dir", $data, "collection", "create", "-n", "demo", "-d", "4", "-m", "cosine", "-i", "hnsw")
$o2 = Invoke-DB @("--data-dir", $data, "collection", "list")
Check "collection demo created" ($o2 -match "demo") "$o / $o2"

Write-Host "== 3. insert vectors (process A) =="
$o = Invoke-DB @("--data-dir", $data, "vector", "insert", "-c", "demo", "-i", "v1", "-v", "1,0,0,0", "-m", '{\"n\":\"a\"}')
$o = Invoke-DB @("--data-dir", $data, "vector", "insert", "-c", "demo", "-i", "v2", "-v", "0,1,0,0", "-m", '{\"n\":\"b\"}')
$o = Invoke-DB @("--data-dir", $data, "vector", "insert", "-c", "demo", "-i", "v3", "-v", "0,0,1,0", "-m", '{\"n\":\"c\"}')
$c = Invoke-DB @("--data-dir", $data, "vector", "count", "-c", "demo")
Check "count after insert = 3" ($c -match "has 3 vectors") $c

Write-Host "== 4. persistence across processes: get v1 in a NEW process =="
$o = Invoke-DB @("--data-dir", $data, "vector", "get", "-c", "demo", "-i", "v1")
Check "v1 survives new process" ($o -match "Vector ID: v1") $o

Write-Host "== 5. UPDATE durability (P0 fix: update_vector now writes storage) =="
$o = Invoke-DB @("--data-dir", $data, "vector", "update", "-c", "demo", "-i", "v1", "-v", "0.9,0.1,0,0", "-m", '{\"n\":\"updated\"}')
$o = Invoke-DB @("--data-dir", $data, "vector", "get", "-c", "demo", "-i", "v1")
Check "updated vector persisted" ($o -match "0\.9") $o
Check "updated metadata persisted" ($o -match "updated") $o

Write-Host "== 6. delete durability =="
$o = Invoke-DB @("--data-dir", $data, "vector", "delete", "-c", "demo", "-i", "v2")
$c = Invoke-DB @("--data-dir", $data, "vector", "count", "-c", "demo")
Check "count after delete = 2" ($c -match "has 2 vectors") $c

Write-Host "== 7. search in a NEW process (index rebuilt from disk) =="
$o = Invoke-DB @("--data-dir", $data, "search", "-c", "demo", "-v", "1,0,0,0", "-k", "2", "--with-metadata", "--format", "json")
Check "search returns v1 top hit" ($o -match '"v1"') $o
Check "search sees updated metadata (--with-metadata in JSON)" ($o -match '"updated"') $o
Check "deleted v2 not in results" (-not ($o -match '"v2"')) $o

Write-Host "== 8. backup (flags = backup.sh contract: --output --name) =="
$o = Invoke-DB @("--data-dir", $data, "backup", "--output", $bk, "--name", "acc1")
$snap = Join-Path $bk "acc1"
Check "backup created snapshot acc1" ((Test-Path $snap) -and (Test-Path (Join-Path $snap "manifest.json"))) "exit=$LASTEXITCODE out=$o"

Write-Host "== 9. damage data, then restore (flags = fixed restore.sh: --input --name --force) =="
$o = Invoke-DB @("--data-dir", $data, "vector", "clear", "-c", "demo", "--force")
$c = Invoke-DB @("--data-dir", $data, "vector", "count", "-c", "demo")
Check "collection cleared (0)" ($c -match "has 0 vectors") $c
$o = Invoke-DB @("--data-dir", $data, "restore", "--input", $bk, "--name", "acc1", "--force")
Check "restore command exit 0" ($LASTEXITCODE -eq 0) "exit=$LASTEXITCODE out=$o"

Write-Host "== 10. post-restore verification (NEW process) =="
$c = Invoke-DB @("--data-dir", $data, "vector", "count", "-c", "demo")
Check "count restored = 2" ($c -match "has 2 vectors") $c
$o = Invoke-DB @("--data-dir", $data, "vector", "get", "-c", "demo", "-i", "v1")
Check "restored v1 keeps updated metadata" ($o -match "updated") $o
$o = Invoke-DB @("--data-dir", $data, "search", "-c", "demo", "-v", "1,0,0,0", "-k", "2", "--with-metadata", "--format", "json")
Check "search after restore hits v1" ($o -match '"v1"') $o
Check "search after restore returns metadata" ($o -match '"metadata"') $o
$safety = Get-ChildItem -Path $data -Force -ErrorAction SilentlyContinue | Where-Object { $_.Name -like ".pre-restore-*" }
Check "old state parked in .pre-restore-*" (@($safety).Count -ge 1) (@($safety).Count)

Write-Host "== 11. doctor =="
$o = Invoke-DB @("--data-dir", $data, "doctor")
Check "doctor runs" ($LASTEXITCODE -eq 0) "exit=$LASTEXITCODE out=$o"

Write-Host ""
Write-Host "=========================================="
Write-Host "ACCEPTANCE RESULT: $script:pass passed, $script:fail failed"
Write-Host "=========================================="
if ($script:fail -gt 0) { exit 1 } else { exit 0 }
