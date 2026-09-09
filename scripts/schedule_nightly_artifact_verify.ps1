#Requires -Version 5.1
<#
.SYNOPSIS
    Register the nightly review-artifact verification as a Windows scheduled task.

.DESCRIPTION
    Context (2026-09-09). The offset verifier's mutation tests used to run against
    the real 3.45 GB / 519,382-row review DB on every test run: 3,396 seconds,
    66.5% of the whole non-GUI suite, paid only on this machine because CI has no
    review DB and skipped the file for free.

    Those mutation tests now run against a synthetic fixture in about a second.
    That change removes 57 minutes from every run, but it also removes the
    incidental full-artifact check that came with it -- and a check that used to
    happen every run must not become a check that happens never.

    This registers the replacement cadence: scripts/verify_review_artifact.py,
    nightly, on the one machine that holds the data. That script FAILS (exit 2)
    when the DB, source keys or corpus file are absent rather than skipping, so a
    missing-data night is visible instead of silently green, and it appends one
    JSON record per run (commit, DB identity, row count, duration, verdict) to
    _tmp/artifact-verify-log.jsonl.

    A nightly green certifies the artifact it ran against, NOT a newly built one.
    Before promoting a rebuilt DB, run the script directly with --expect-rows.

.PARAMETER Time
    Local time to run, HH:mm. Default 03:00.

.PARAMETER TaskName
    Scheduled-task name. Default GenizahSearch-VerifyReviewArtifact.

.PARAMETER WhatIfOnly
    Print the task definition and exit without registering anything.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\schedule_nightly_artifact_verify.ps1 -WhatIfOnly
    powershell -ExecutionPolicy Bypass -File scripts\schedule_nightly_artifact_verify.ps1
#>
param(
    [string]$Time = "03:00",
    [string]$TaskName = "GenizahSearch-VerifyReviewArtifact",
    [switch]$WhatIfOnly
)

$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $PSScriptRoot
$script = Join-Path $repo "scripts\verify_review_artifact.py"
$logDir = Join-Path $repo "_tmp"
$jsonLog = Join-Path $logDir "artifact-verify-log.jsonl"
$textLog = Join-Path $logDir "artifact-verify-last.txt"

if (-not (Test-Path $script)) {
    throw "not found: $script"
}

$python = (Get-Command python -ErrorAction SilentlyContinue)
if ($null -eq $python) { throw "python is not on PATH" }
$pythonPath = $python.Source

# -u so the log is written as it goes rather than on exit, and the text log keeps
# the LAST run's full output while the jsonl keeps every run's verdict.
$argLine = ('-X utf8 -u "{0}" --json-out "{1}"' -f $script, $jsonLog)
$wrapped = ('/c ""{0}" {1}" > "{2}" 2>&1' -f $pythonPath, $argLine, $textLog)

Write-Output "repo        : $repo"
Write-Output "task        : $TaskName"
Write-Output "time        : $Time (daily)"
Write-Output "python      : $pythonPath"
Write-Output "json log    : $jsonLog"
Write-Output "text log    : $textLog"
Write-Output ""
Write-Output "command     : cmd.exe $wrapped"
Write-Output ""

if ($WhatIfOnly) {
    Write-Output "-WhatIfOnly given: nothing registered."
    exit 0
}

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existing) {
    Write-Output "A task named '$TaskName' already exists. Not overwriting it."
    Write-Output "Remove it first if you mean to replace it:"
    Write-Output "  Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
    exit 1
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $wrapped -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Daily -At $Time
# Deliberately NOT WakeToRun and NOT RunOnlyIfNetworkAvailable: the check is
# entirely local, and a machine that was asleep should show a MISSED night
# rather than be woken for it.
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Description ("Nightly full verification of " +
    "discovery-v5-REVIEW.db offsets. Fails on missing data rather than " +
    "skipping. See scripts/verify_review_artifact.py.") | Out-Null

Write-Output "Registered. Verify with:"
Write-Output "  Get-ScheduledTask -TaskName '$TaskName' | Format-List TaskName,State"
Write-Output "Run it once now with:"
Write-Output "  Start-ScheduledTask -TaskName '$TaskName'"
Write-Output ""
Write-Output "A failed or missed night must stay visible -- check $jsonLog"
Write-Output "and treat a gap in its dates as an unverified artifact."
