"""OS allocation limits installed in disposable search workers, before engines.

Linux RLIMIT_DATA also covers anonymous mmap allocations (Linux >= 4.7), while
allowing read-only index files to be mapped. Windows Job Objects bound committed
memory across the job. The parent additionally watches RSS and host free memory.
"""
import os
from pathlib import Path

import psutil

_job_handle = None


def available_memory():
    """Respect a Linux service/container cgroup, not only host-wide free RAM."""
    available = psutil.virtual_memory().available
    if os.name == 'nt':
        return available
    return _cgroup_available_memory(available, Path('/sys/fs/cgroup'), Path('/proc/self/cgroup'))


def _cgroup_available_memory(available, root, membership_path):
    """Apply limits from the membership's controller mount and its ancestors."""
    directories = [root, root / 'memory']
    try:
        for line in membership_path.read_text().splitlines():
            _, controllers, relative = line.split(':', 2)
            if controllers == '' or 'memory' in controllers.split(','):
                mount = root if controllers == '' else root / 'memory'
                directory = mount / relative.lstrip('/')
                while directory == mount or mount in directory.parents:
                    directories.append(directory)
                    if directory == mount:
                        break
                    directory = directory.parent
    except (OSError, ValueError):
        pass
    for directory in set(directories):
        for limit_name, used_name in [('memory.max', 'memory.current'),
                                      ('memory.limit_in_bytes', 'memory.usage_in_bytes')]:
            try:
                limit = int((directory / limit_name).read_text())
                used = int((directory / used_name).read_text())
                available = min(available, max(0, limit - used))
            except (OSError, ValueError):
                pass
    return available


def limit_memory(byte_limit):
    if os.name != 'nt':
        import resource
        _, hard = resource.getrlimit(resource.RLIMIT_DATA)
        limit = min(byte_limit, hard) if hard != resource.RLIM_INFINITY else byte_limit
        resource.setrlimit(resource.RLIMIT_DATA, (limit, limit))
        return

    import ctypes
    from ctypes import wintypes as w

    class Basic(ctypes.Structure):
        _fields_ = [('ProcessTime', ctypes.c_longlong), ('JobTime', ctypes.c_longlong),
                    ('Flags', w.DWORD), ('MinWorkingSet', ctypes.c_size_t),
                    ('MaxWorkingSet', ctypes.c_size_t), ('ActiveProcesses', w.DWORD),
                    ('Affinity', ctypes.c_size_t), ('Priority', w.DWORD), ('Scheduling', w.DWORD)]

    class IO(ctypes.Structure):
        _fields_ = [(name, ctypes.c_ulonglong) for name in
                    ('ReadOps', 'WriteOps', 'OtherOps', 'ReadBytes', 'WriteBytes', 'OtherBytes')]

    class Extended(ctypes.Structure):
        _fields_ = [('Basic', Basic), ('IO', IO), ('ProcessMemory', ctypes.c_size_t),
                    ('JobMemory', ctypes.c_size_t), ('PeakProcessMemory', ctypes.c_size_t),
                    ('PeakJobMemory', ctypes.c_size_t)]

    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, w.LPCWSTR]
    kernel.CreateJobObjectW.restype = w.HANDLE
    kernel.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
    kernel.SetInformationJobObject.restype = w.BOOL
    kernel.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
    kernel.AssignProcessToJobObject.restype = w.BOOL
    kernel.GetCurrentProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    handle = kernel.CreateJobObjectW(None, None)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    limits = Extended()
    limits.Basic.Flags = 0x100 | 0x200 | 0x2000  # process/job memory; kill-on-close
    limits.ProcessMemory = limits.JobMemory = byte_limit
    if not kernel.SetInformationJobObject(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        error = ctypes.WinError(ctypes.get_last_error())
        kernel.CloseHandle(handle)
        raise error
    if not kernel.AssignProcessToJobObject(handle, kernel.GetCurrentProcess()):
        error = ctypes.WinError(ctypes.get_last_error())
        kernel.CloseHandle(handle)
        raise error
    global _job_handle
    _job_handle = handle  # retain until process exit
