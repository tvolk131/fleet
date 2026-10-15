"""Fixed guest-side helpers. Data arrives as JSON arguments, never shell text.

The host treats this entire guest, including this helper's replies, as untrusted.
Host-side byte/time limits and VM isolation remain authoritative.
"""
import hashlib
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile

spec = json.loads(sys.argv[1])
kind = spec['kind']
if kind == 'ready':
    print('fleet-guest-v1')
elif kind == 'exec':
    env = dict(os.environ)
    env.update(spec['env'])
    child = subprocess.Popen(spec['argv'], cwd=spec['cwd'], env=env,
                             stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        code = child.wait(timeout=spec['timeout_seconds'])
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        print('Fleet guest command timed out', file=sys.stderr)
        sys.exit(124)
    sys.exit(code if code >= 0 else min(255, 128-code))
elif kind == 'upload':
    path = spec['path']
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.fleet-upload-', dir=parent)
    try:
        digest = hashlib.sha256()
        remaining = spec['size']
        with os.fdopen(fd, 'wb') as out:
            while remaining:
                data = sys.stdin.buffer.read(min(65536, remaining))
                if not data:
                    raise RuntimeError('incomplete upload')
                digest.update(data)
                out.write(data)
                remaining -= len(data)
            if sys.stdin.buffer.read(1):
                raise RuntimeError('upload exceeded declared length')
            if digest.hexdigest() != spec['sha256']:
                raise RuntimeError('upload SHA-256 mismatch')
            os.fchmod(out.fileno(), 0o700 if spec['executable'] else 0o600)
            out.flush()
            os.fsync(out.fileno())
        # A hard link creates the destination atomically without overwriting.
        # Replaying the same upload is harmless when identical content is present.
        try:
            os.link(temporary, path)
        except FileExistsError:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as current:
                meta = os.fstat(current.fileno())
                if not stat.S_ISREG(meta.st_mode) or meta.st_size != spec['size']:
                    raise RuntimeError('destination exists with different content')
                other = hashlib.sha256()
                for chunk in iter(lambda: current.read(65536), b''):
                    other.update(chunk)
                if other.hexdigest() != spec['sha256']:
                    raise RuntimeError('destination exists with different content')
                if bool(meta.st_mode & 0o111) != spec['executable']:
                    raise RuntimeError('destination exists with different executable permissions')
        directory = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        print(json.dumps({'bytes': spec['size'], 'sha256': digest.hexdigest()}))
    finally:
        os.unlink(temporary)
elif kind == 'download':
    def version(meta):
        return ':'.join(str(v) for v in (meta.st_dev, meta.st_ino, meta.st_size,
                                        meta.st_mtime_ns, meta.st_ctime_ns))
    expected = spec.get('version')
    try:
        fd = os.open(spec['path'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if expected: sys.exit(75)
        raise
    with os.fdopen(fd, 'rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError('download requires a regular file, not a symlink or device')
        if expected and version(before) != expected:
            sys.exit(75)
        if before.st_size > spec['max_bytes']:
            raise RuntimeError('download exceeds byte limit')
        remaining = before.st_size
        while remaining:
            data = source.read(min(remaining, 65536))
            if not data:
                sys.exit(75)
            sys.stdout.buffer.write(data)
            remaining -= len(data)
        after = os.fstat(source.fileno())
        if version(after) != version(before):
            sys.exit(75)

else:
    raise RuntimeError('unknown guest operation')
