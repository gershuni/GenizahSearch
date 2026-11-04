<#
.SYNOPSIS
    Name every reparse point (junction, symbolic link) inside a git worktree, so the
    cleanup step sees them BEFORE `git worktree remove --force` or `rm -rf`.

.DESCRIPTION
    On 2026-11-04 a worktree under .claude\worktrees\ held junctions to the main
    checkout's gitignored data folders (nli_data, fgp_data, pgp_data, fist_data), made
    so its dev server would find the sidecars. `git worktree remove --force` and MSYS
    `rm -rf` walk a junction as a directory, so removing that worktree emptied the main
    checkout's nli_data\ and fgp_data\. This script is the check scripts/init.ps1 runs
    at every session start and the cleanup step must run before any forced removal.

    It walks each folder given (default: the current folder when it is a linked
    worktree, otherwise every folder under <repo>\.claude\worktrees\) to any depth,
    never stepping INTO a reparse point, and prints one line per finding: the path,
    its kind and its target. Exit code 1 when any is found, 0 when none. A folder it
    cannot read (ACL, I/O error, a -Path that is not a folder) is printed as "could not
    scan" and the exit code is 2: an incomplete scan is never reported clean. It
    changes nothing. (Codex on the PR: round 1, a junction two levels down was missed
    by a version that looked one level down only; round 2, an unreadable subtree was
    skipped silently and the scan still said clean; round 3, the same for the
    .claude\worktrees folder itself during default discovery.)

    Remove a junction with `cmd /c rmdir <path>` (no /S): that deletes the link and
    leaves its target alone. Never Remove-Item -Recurse, rm -rf or git worktree remove
    --force while one exists. A worktree that needs a sidecar copies it under
    %LOCALAPPDATA%\GenizahSearchPro\data\<dir>\ instead (CLAUDE.md, "How to work here").

.EXAMPLE
    powershell -File scripts/check_worktree_links.ps1
    powershell -File scripts/check_worktree_links.ps1 -Path .claude\worktrees\browse-zoom
#>
param(
    [string[]]$Path,
    [switch]$Quiet      # findings only, no summary line (scripts/init.ps1 uses this)
)

$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$script:unscanned = @()   # folders the walk could not read: the scan is then incomplete

function Test-LinkedWorktree([string]$Folder) {
    # A linked worktree's .git is a FILE ("gitdir: ..."); the main checkout's is a folder.
    return (Test-Path -LiteralPath (Join-Path $Folder '.git') -PathType Leaf)
}

function Get-ReparsePoints([string]$Root) {
    # Every ordinary directory under $Root, to any depth, by an explicit stack: a reparse
    # point is RECORDED and never entered (listing a junction's target is the walk this
    # script exists to prevent), and Get-ChildItem -Recurse is not used because Windows
    # PowerShell 5.1 follows junctions when it recurses.
    $found = @()
    $stack = New-Object System.Collections.Stack
    try { $stack.Push((Get-Item -LiteralPath $Root -Force -ErrorAction Stop)) }
    catch { $script:unscanned += ('{0}: {1}' -f $Root, $_.Exception.Message); return @() }
    while ($stack.Count -gt 0) {
        $dir = $stack.Pop()
        $entries = @()
        try { $entries = @($dir.GetFileSystemInfos()) }
        catch {
            # Fail closed: a subtree we could not list may hold a junction.
            $script:unscanned += ('{0}: {1}' -f $dir.FullName, $_.Exception.Message)
            continue
        }
        foreach ($e in $entries) {
            if ($e.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                $found += $e
            } elseif ($e.Attributes -band [IO.FileAttributes]::Directory) {
                $stack.Push($e)
            }
        }
    }
    return @($found | Sort-Object FullName)
}

if (-not $Path) {
    $here = (Get-Location).Path
    if (Test-LinkedWorktree $here) {
        $Path = @($here)
    } else {
        $wt = Join-Path (Split-Path $PSScriptRoot -Parent) '.claude\worktrees'
        $Path = @()
        if (Test-Path -LiteralPath $wt -PathType Container) {
            try {
                $Path = @(Get-ChildItem -LiteralPath $wt -Force -Directory -ErrorAction Stop |
                          ForEach-Object { $_.FullName })
            } catch {
                # Fail closed (Codex, round 3): a worktrees folder we cannot list may hold a
                # worktree with a junction, so this is not "no worktrees".
                $script:unscanned += ('{0}: {1}' -f $wt, $_.Exception.Message)
            }
        }
    }
}

$total = 0
foreach ($p in $Path) {
    if (-not (Test-Path -LiteralPath $p -PathType Container)) {
        $script:unscanned += ('{0}: not a folder' -f $p)
        continue
    }
    $full = (Resolve-Path -LiteralPath $p).Path
    foreach ($item in (Get-ReparsePoints $full)) {
        $total += 1
        $kind = $item.LinkType
        if (-not $kind) { $kind = 'ReparsePoint' }
        $target = $item.Target
        if (-not $target) { $target = '(target unknown)' }
        Write-Output ('  {0}  [{1}] -> {2}' -f $item.FullName, $kind, ($target -join ', '))
    }
}

foreach ($u in $script:unscanned) { Write-Output ('  could not scan {0}' -f $u) }
$problems = $script:unscanned.Count

if ($total -eq 0 -and $problems -eq 0) {
    if (-not $Quiet) { Write-Output ('no reparse points under: ' + ($Path -join '; ')) }
    exit 0
}
if (-not $Quiet) {
    Write-Output ''
    if ($problems -gt 0) {
        Write-Output ('{0} folder(s) could not be read: the scan is INCOMPLETE and the worktree is not known clean.' -f $problems)
    }
    if ($total -gt 0) {
        Write-Output ('{0} reparse point(s). Remove each with "cmd /c rmdir <path>" (no /S), never recursively,' -f $total)
        Write-Output 'and never "git worktree remove --force" or "rm -rf" a worktree while one exists: both walk'
        Write-Output 'the link into its target (2026-11-04: the main checkout''s nli_data\ and fgp_data\ were emptied).'
    }
}
if ($total -gt 0) { exit 1 }
exit 2
