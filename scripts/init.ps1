<#
.SYNOPSIS
    Session-start orientation for GenizahSearch. Run this instead of reading docs.

.DESCRIPTION
    Prints, in one screen: repo state, milestone state, the tracker's newest entry and
    open-item count, the most recent working notes, and the exit code of every gate.

    This exists because session-start re-orientation used to mean reading CLAUDE.md +
    docs/OPEN_ISSUES.md in full (~135k resident tokens before any work began). The
    files are now split and capped; this script covers the rest, and its output is a
    few hundred tokens.

.EXAMPLE
    powershell -File scripts/init.ps1
    powershell -File scripts/init.ps1 -NoGates    # skip the gate run (faster)
#>
param([switch]$NoGates)

$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Split-Path $PSScriptRoot -Parent)

function Section([string]$t) {
    Write-Host ""
    Write-Host ("== " + $t + " " + ("=" * [Math]::Max(0, 58 - $t.Length))) -ForegroundColor Cyan
}

Write-Host ""
Write-Host "GenizahSearch - session start  ($(Get-Date -Format 'yyyy-MM-dd HH:mm'))" -ForegroundColor White

# ---------------------------------------------------------------- repo ----
Section "Repo"
$branch = (git rev-parse --abbrev-ref HEAD)
$dirty  = @(git status --porcelain)
Write-Host ("branch      : {0}" -f $branch)
Write-Host ("uncommitted : {0} file(s)" -f $dirty.Count)
if ($dirty.Count -gt 0 -and $dirty.Count -le 12) { $dirty | ForEach-Object { Write-Host ("              " + $_) } }
Write-Host "recent commits:"
git log --oneline -5 | ForEach-Object { Write-Host ("  " + $_) }

# ----------------------------------------------------------- milestone ----
Section "Milestone (.planning/STATE.md frontmatter)"
if (Test-Path '.planning\STATE.md') {
    $st = Get-Content '.planning\STATE.md' -Encoding UTF8 -TotalCount 40
    $inFm = $false
    foreach ($l in $st) {
        if ($l -match '^---\s*$') { if ($inFm) { break }; $inFm = $true; continue }
        if ($inFm -and $l -match '^(milestone|milestone_name|status|stopped_at|last_activity|  (total_phases|completed_phases|total_plans|completed_plans|percent))') {
            Write-Host ("  " + $l)
        }
    }
} else {
    Write-Host "  (no .planning/STATE.md)"
}

# ------------------------------------------------------------- tracker ----
Section "Open issues tracker"
$oi = 'docs\OPEN_ISSUES.md'
if (Test-Path $oi) {
    $lines  = Get-Content $oi -Encoding UTF8
    $bytes  = (Get-Item $oi).Length
    $openN  = @($lines | Where-Object { $_ -match [char]0x274C }).Count
    $pendN  = @($lines | Where-Object { $_ -match [char]0x23F3 }).Count
    Write-Host ("  size        : {0:N0} bytes / 180,000 ceiling  ({1:N0}%)" -f $bytes, (100 * $bytes / 180000))
    Write-Host ("  open marks  : {0} open, {1} pending" -f $openN, $pendN)
    $lu = $lines | Where-Object { $_ -match '^\>\s*\*\*Last Updated' } | Select-Object -First 1
    if ($lu) {
        $txt = ($lu -replace '^\>\s*', '') -replace '\s+', ' '
        if ($txt.Length -gt 400) { $txt = $txt.Substring(0, 400) + ' ...' }
        Write-Host "  newest entry:"
        Write-Host ("    " + $txt) -ForegroundColor Gray
    }
    Write-Host "  (read the Quick Summary + the one relevant section. Grep the rest." -ForegroundColor DarkGray
    Write-Host "   Closed history is docs/archive/OPEN_ISSUES_ARCHIVE.md - grep only.)" -ForegroundColor DarkGray
}

# ------------------------------------------------------- working notes ----
Section "Most recent working notes (_tmp/, untracked)"
if (Test-Path '_tmp') {
    Get-ChildItem '_tmp' -Filter *.md -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 6 |
        ForEach-Object { Write-Host ("  {0:yyyy-MM-dd HH:mm}  {1}" -f $_.LastWriteTime, $_.Name) }
} else {
    Write-Host "  (no _tmp/)"
}

# --------------------------------------------------------------- gates ----
if ($NoGates) {
    Section "Gates"
    Write-Host "  skipped (-NoGates)"
} else {
    Section "Gates"
    $env:PYTHONUTF8 = '1'
    # Relative path on purpose: .claude/settings.json denies Read on this file so its
    # restricted patterns never reach the model's context, and resolving it to an
    # absolute path trips that deny. The gate script reads it directly, which is fine --
    # we only need to name it, not open it. Set-Location above puts us at the repo root.
    if (Test-Path '.masking_patterns') {
        $env:MASKING_SCAN_PATTERNS_FILE = '.masking_patterns'
    }

    $results = @()

    if (-not $env:MASKING_SCAN_PATTERNS_FILE) {
        $results += , @('masking --scan-repo', 'SKIPPED', 'no .masking_patterns - the gate fails closed by design, it is not green')
    } else {
        python scripts\check_atlas_masking.py --scan-repo *> $null
        $results += , @('masking --scan-repo', $LASTEXITCODE, '')
    }

    python scripts\check_docs.py *> $null
    $results += , @('check_docs.py', $LASTEXITCODE, '')

    $ruff = Get-Command ruff -ErrorAction SilentlyContinue
    if ($ruff) {
        ruff check . *> $null
        $results += , @('ruff check', $LASTEXITCODE, '')
    } else {
        $results += , @('ruff check', 'SKIPPED', 'ruff not on PATH')
    }

    foreach ($r in $results) {
        $name = $r[0]; $code = $r[1]; $note = $r[2]
        if ($code -eq 0) {
            Write-Host ("  [ ok ] {0,-22} exit 0 {1}" -f $name, $note) -ForegroundColor Green
        } elseif ($code -eq 'SKIPPED') {
            Write-Host ("  [skip] {0,-22}        {1}" -f $name, $note) -ForegroundColor Yellow
        } else {
            Write-Host ("  [FAIL] {0,-22} exit {1} {2}" -f $name, $code, $note) -ForegroundColor Red
        }
    }
}

Write-Host ""
Write-Host "Do NOT read whole: docs/archive/OPEN_ISSUES_ARCHIVE.md (~320 KB), CHANGELOG.md (~227 KB)," -ForegroundColor DarkGray
Write-Host ".planning/STATE.md (~106 KB). Grep them." -ForegroundColor DarkGray
Write-Host ""
