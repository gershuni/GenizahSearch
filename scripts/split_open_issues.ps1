param([switch]$Apply)

# Splits docs/OPEN_ISSUES.md into a small session-start tracker + a full archive.
# Rule: terminal-status entries (Fixed/Done/Resolved/Superseded/Won't Fix) -> archive.
#       Actionable/unresolved entries (Open/Pending/Recorded/Deferred/Known limit) -> stay.
# Nothing is deleted; every archived line is written to the archive file.

$ErrorActionPreference = 'Stop'
$src  = 'docs\OPEN_ISSUES.md'
$cur  = 'docs\OPEN_ISSUES.md'
$arch = 'docs\archive\OPEN_ISSUES_ARCHIVE.md'

$lines = [System.IO.File]::ReadAllLines((Resolve-Path $src), [System.Text.Encoding]::UTF8)
$n = $lines.Count
$cls = New-Object string[] $n
for ($i = 0; $i -lt $n; $i++) { $cls[$i] = 'keep' }

# ---- terminal-status detection (encoding-robust: words, not glyphs) ----
function Test-Terminal([string]$s) {
    if ($s -match "Won'?t Fix")                                   { return $true }
    if ($s -match '\b(Fixed|Resolved|Superseded|Improved|Addressed)\b') { return $true }
    if ($s -match '\bDone\b')                                     { return $true }
    if ($s -match 'Checked and clean')                            { return $true }
    return $false
}
function Test-Live([string]$s) {
    if ($s -match '\bOpen\b')          { return $true }
    if ($s -match '\bPending\b')       { return $true }
    if ($s -match '\bRecorded\b')      { return $true }
    if ($s -match '\bDeferred\b')      { return $true }
    if ($s -match '\bKnown limit\b')   { return $true }
    if ($s -match '\bNOT MET\b')       { return $true }
    return $false
}
# A line is archived when it is terminal AND not also live. Live always wins (fail-closed:
# when a row says both, we keep it in the current file rather than risk hiding open work).
function Get-Class([string]$s) {
    if (Test-Live $s)     { return 'keep' }
    if (Test-Terminal $s) { return 'arch' }
    return 'keep'
}

# ---- section boundaries (recomputed, not hardcoded) ----
$secStart = @{}
for ($i = 0; $i -lt $n; $i++) {
    if ($lines[$i] -match '^##\s+(.*)') { $secStart[$Matches[1]] = $i }
}
function Get-SectionRange([string]$titleLike) {
    $hit = $secStart.Keys | Where-Object { $_ -like $titleLike } | Select-Object -First 1
    if (-not $hit) { return $null }
    $s = $secStart[$hit]
    $e = $n - 1
    for ($j = $s + 1; $j -lt $n; $j++) { if ($lines[$j] -match '^##\s') { $e = $j - 1; break } }
    return @($s, $e, $hit)
}

# ---- 1. v9 Discovery section ----
# Top-level bullet = the finding's headline; classified on its STATUS PREFIX only.
# Nested sub-bullets = deep narrative; always archived (they stay readable in the
# archive underneath their parent). The header of this section already declares
# `.planning/STATE.md` the authoritative tracker, not this file.
$r = Get-SectionRange 'v9.0.0 Discovery Milestone*'
if ($r) {
    $unit = 'keep'
    for ($i = $r[0] + 1; $i -le $r[1]; $i++) {
        $l = $lines[$i]
        if ($l -match '^-\s') {
            $head = $l.Substring(0, [Math]::Min(140, $l.Length))
            $unit = Get-Class $head; $cls[$i] = $unit; continue
        }
        if ($l -match '^\s+\S') {
            # nested narrative -> archive, UNLESS the sub-bullet carries its own live
            # status (an open item can sit under a closed parent; line 138 did).
            $head = $l.Substring(0, [Math]::Min(150, $l.Length))
            $cls[$i] = if (Test-Live $head) { 'keep' } else { 'arch' }
            continue
        }
        $cls[$i] = 'keep'
    }
}

# ---- 2. Outstanding Bugs / Deferred backlog ----
# Classify on the STATUS COLUMN only. Judging the whole row keeps every "Fixed" entry
# whose notes happen to contain the word "open".
foreach ($pat in @('1. Outstanding Bugs', 'Deferred to v7.15+*')) {
    $r = Get-SectionRange $pat
    if (-not $r) { continue }
    for ($i = $r[0] + 1; $i -le $r[1]; $i++) {
        $l = $lines[$i]
        if ($l -notmatch '^\|' -or $l -match '^\|\s*[-: ]+\|') { continue }
        $f = $l.Split('|')
        # | (0 empty) | title(1) | location(2) | status(3) | notes(4..) |
        $status = if ($f.Count -gt 3) { $f[3] } else { $l }
        $cls[$i] = Get-Class $status
    }
}

# ---- 3. Change Log + Completed Issues: wholesale archive (pure history) ----
foreach ($pat in @('Change Log', '8. Completed Issues*')) {
    $r = Get-SectionRange $pat
    if ($r) { for ($i = $r[0] + 1; $i -le $r[1]; $i++) { $cls[$i] = 'arch' } }
}

# ---- 4. the "Previous (...)" status blockquote at the top ----
for ($i = 0; $i -lt [Math]::Min(12, $n); $i++) {
    if ($lines[$i] -match '^>\s*Previous \(') { $cls[$i] = 'arch' }
}

# ---- report ----
$keepB = 0; $archB = 0; $keepN = 0; $archN = 0
for ($i = 0; $i -lt $n; $i++) {
    $b = $lines[$i].Length + 1
    if ($cls[$i] -eq 'arch') { $archB += $b; $archN++ } else { $keepB += $b; $keepN++ }
}
"source      : {0,9:N0} bytes / {1,4} lines" -f ($keepB + $archB), $n
"current  -> : {0,9:N0} bytes / {1,4} lines" -f $keepB, $keepN
"archive  -> : {0,9:N0} bytes / {1,4} lines" -f $archB, $archN
"approx tokens saved per session: {0,7:N0}" -f ($archB / 4)
""
"--- bytes STAYING, by section ---"
$sec = '(top)'; $per = @{}
for ($i = 0; $i -lt $n; $i++) {
    if ($lines[$i] -match '^##\s+(.*)') { $sec = $Matches[1] }
    if (-not $per.ContainsKey($sec)) { $per[$sec] = 0 }
    if ($cls[$i] -eq 'keep') { $per[$sec] += $lines[$i].Length + 1 }
}
$per.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 8 |
    ForEach-Object { $t = $_.Key; if ($t.Length -gt 60) { $t = $t.Substring(0, 60) }; "{0,9:N0}  {1}" -f $_.Value, $t }

if (-not $Apply) { "`n(dry run - nothing written; pass -Apply to write)"; exit 0 }

# GUARD (2026-09-06): this script REBUILDS the archive from scratch with WriteAllLines. It was
# written for the first split on 2026-08-12, when no archive existed. Re-running -Apply would
# replace the ~515 KB archive with this run's few-KB yield. Use scripts/archive_closed_issues.py,
# which appends. The dry run above is still safe.
if (Test-Path $arch) {
    Write-Error "Refusing -Apply: $arch already exists and this script would overwrite it. Run: python scripts/archive_closed_issues.py --apply"
    exit 2
}

# ---- write ----
$stamp = (git log -1 --format=%cd --date=short) 2>$null
$archHdr = @(
    '# OPEN_ISSUES — Archive',
    '',
    "> Split out of `docs/OPEN_ISSUES.md` on 2026-08-12. Everything here is **closed**:",
    '> fixed, resolved, superseded, or historical change-log narrative. Nothing was deleted —',
    '> these lines were moved verbatim so the session-start tracker stays small.',
    '>',
    '> Open and unresolved items live in [`docs/OPEN_ISSUES.md`](../OPEN_ISSUES.md).',
    '> Search this file with grep rather than reading it.',
    ''
)
$archLines = New-Object System.Collections.Generic.List[string]
foreach ($h in $archHdr) { $archLines.Add($h) }
$keepLines = New-Object System.Collections.Generic.List[string]
$lastArchSection = ''
for ($i = 0; $i -lt $n; $i++) {
    if ($lines[$i] -match '^##\s') { $lastArchSection = $lines[$i] }
    if ($cls[$i] -eq 'arch') {
        if ($archLines[$archLines.Count - 1] -notmatch '^##\s' -and $lastArchSection -ne '' -and
            ($archLines -notcontains $lastArchSection)) { $archLines.Add(''); $archLines.Add($lastArchSection); $archLines.Add('') }
        $archLines.Add($lines[$i])
    } else {
        $keepLines.Add($lines[$i])
    }
}
New-Item -ItemType Directory -Force -Path 'docs\archive' | Out-Null
$utf8 = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines((Join-Path (Get-Location) $arch), $archLines, $utf8)
[System.IO.File]::WriteAllLines((Join-Path (Get-Location) $cur),  $keepLines, $utf8)
"written: $cur and $arch"
