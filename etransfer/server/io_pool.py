"""Shared I/O thread pool for the EasyTransfer server.

Both TUS storage (upload writes) and file download routes use
``os.pread`` / ``os.pwrite`` which are blocking syscalls.  Running
them in a shared ``ThreadPoolExecutor`` keeps the event loop free
without creating redundant pools.
"""

from concurrent.futures import ThreadPoolExecutor

io_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="et-io")
