import sys
import os
import multiprocessing as mp
import psutil

def check_if_there_is_no_huge_library_imported():
    for lib in ['pandas', 'numpy', 'alive_progress']:
        if lib in list(sys.modules.keys()):
            raise Exception(f'Imported {lib} whilst running data sink. Check the imports')

def debug_where_am_i(tag: str = "") -> str:
    pid = os.getpid()
    proc_name = mp.current_process().name

    affinity = None
    if psutil is not None:
        try:
            affinity = psutil.Process(pid).cpu_affinity()
        except Exception:
            affinity = None

    core = None
    try:
        import ctypes
        core = ctypes.windll.kernel32.GetCurrentProcessorNumber()
    except Exception:
        core = None

    return f"[{tag}] pid={pid} proc={proc_name} core={core} affinity={affinity}"
