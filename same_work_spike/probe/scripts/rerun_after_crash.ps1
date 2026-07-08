# 2026-07-08 crash-recovery runner: PC hard-crashed at 10:10 killing
# both overnight jobs mid-run. Rerun SEQUENTIALLY (they were contending
# before the crash) at BelowNormal priority. Both scripts now
# checkpoint, so a further crash resumes instead of restarting.
$scripts = 'C:\Genizahsearch\same_work_spike\probe\scripts'
$logs = 'C:\Genizahsearch\same_work_spike\probe\results\overnight'
$env:PYTHONFAULTHANDLER = '1'

$p = Start-Process python -ArgumentList '-X', 'utf8', '-u', 'motif_query.py' `
    -WorkingDirectory $scripts -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput "$logs\motif-query-r2.log" `
    -RedirectStandardError "$logs\motif-query-r2.err.log"
try { $p.PriorityClass = 'BelowNormal' } catch {}
$p.WaitForExit()

$p2 = Start-Process python -ArgumentList '-X', 'utf8', '-u', 'mask_ref_canon.py' `
    -WorkingDirectory $scripts -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput "$logs\mask-ref-canon-v2-r2.log" `
    -RedirectStandardError "$logs\mask-ref-canon-v2-r2.err.log"
try { $p2.PriorityClass = 'BelowNormal' } catch {}
$p2.WaitForExit()
