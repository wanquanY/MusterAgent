"""Stop every workload process, then stream the tmpfs without a disk copy.
All workload processes have this UID and cannot change it (capabilities dropped).
The collector is a fresh Docker exec, outside their process groups. The host
bounds both this operation's duration and the number of bytes received.
"""
import os
import ctypes
import struct
from pathlib import Path
import signal
import sys
import tarfile

# This map is built before workload execution. Birth times distinguish inode
# reuse from renames; inode numbers alone are insufficient.
_libc = ctypes.CDLL(None, use_errno=True)

def identity(path):
    info = os.lstat(path)
    data = ctypes.create_string_buffer(256)
    # statx(AT_FDCWD, path, AT_SYMLINK_NOFOLLOW, STATX_INO | STATX_BTIME, buf)
    if _libc.statx(-100, os.fsencode(path), 0x100, 0x900, data) != 0:
        raise OSError(ctypes.get_errno(), 'statx failed')
    if not struct.unpack_from('I', data.raw)[0] & 0x800:
        raise RuntimeError('workspace filesystem must provide inode birth times')
    return (info.st_dev, info.st_ino, data.raw[80:96])

original = {}
for path in Path('/sandbox/work').rglob('*'):
    if not path.is_symlink():
        original[identity(path)] = path.relative_to('/sandbox/work').as_posix()

def provenance(header):
    if header.isfile() or header.isdir():
        path = Path('/sandbox') / header.name
        prior = original.get(identity(path))
        if prior is not None:
            header.pax_headers['musteragent.original-path'] = prior
    return header

sys.stdout.buffer.write(b'R')
sys.stdout.buffer.flush()
if sys.stdin.buffer.read(1) != b'C':
    sys.exit(1)
own = os.getpid()
stopped = set()
for _ in range(32):
    found = set()
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit() or int(entry.name) in (1, own):
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
            pid = int(entry.name)
            os.kill(pid, signal.SIGSTOP)
            found.add(pid)
        except (ProcessLookupError, FileNotFoundError):
            pass
    if found <= stopped:
        break
    stopped |= found
else:
    raise RuntimeError('could not freeze workload processes')
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|', dereference=False) as archive:
    archive.add('/sandbox/work', arcname='work', recursive=True, filter=provenance)
