"""Fixed, read-only guest observation plus work-folder preparation.
Replies are untrusted; the host validates shapes, counts, paths and byte limits.
"""
import json
import math
import os
import re
import stat
import sys
import time

spec = json.loads(sys.argv[1])
root = spec['root']


def matches(pattern, path, exact=False):
    parts, names = pattern.split('/'), path.split('/')
    return (len(parts) == len(names) if exact else len(parts) <= len(names)) and all(
        re.fullmatch(re.escape(p).replace(r'\*', '.*'), n) is not None
        for p, n in zip(parts, names))


def version(meta):
    return ':'.join(str(v) for v in (meta.st_dev, meta.st_ino, meta.st_size,
                                    meta.st_mtime_ns, meta.st_ctime_ns))


def values(path, field, points):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as source:
            meta = os.fstat(source.fileno())
            if not stat.S_ISREG(meta.st_mode):
                return []
            start = max(0, meta.st_size - 512 * 1024)
            source.seek(start)
            data = source.read(512 * 1024)
        if start:
            data = data.partition(b'\n')[2]
        # An in-progress trailing line is not a sample yet.
        data = data[:data.rfind(b'\n') + 1]
        result = []
        for line in reversed(data.splitlines()):
            try:
                value = json.loads(line)
                for key in field.split('.'):
                    value = value[key]
                if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                    result.append(value)
                    if len(result) >= points:
                        break
            except (ValueError, KeyError, TypeError, OverflowError):
                pass
        return list(reversed(result)) if points else []
    except (FileNotFoundError, NotADirectoryError):
        return []


if spec['kind'] == 'prepare':
    if spec.get('platform'):
        import platform
        actual = {'os': {'Darwin': 'macos'}.get(platform.system(), platform.system().lower()),
                  'arch': {'arm64': 'aarch64'}.get(platform.machine(), platform.machine())}
        if any(actual[k] != spec['platform'][k] for k in ('os', 'arch')):
            raise RuntimeError('guest OS/architecture differs from portable runtime: ' + str(actual))
    os.makedirs(root, mode=0o700, exist_ok=True)
elif spec['kind'] in ('manifest', 'observe'):
    paths, result, total, visited = [], [], 0, 0
    for parent, directories, files in os.walk(root, followlinks=False):
        directories[:] = sorted(d for d in directories if not os.path.islink(os.path.join(parent, d)))
        visited += len(directories) + len(files)
        if visited > 100000:
            raise RuntimeError('output inventory exceeded its scan limit')
        for name in sorted(directories + files):
            absolute = os.path.join(parent, name)
            relative = os.path.relpath(absolute, root)
            try:
                metadata = os.lstat(absolute)
            except FileNotFoundError:
                continue
            paths.append((relative, metadata))
            if stat.S_ISDIR(metadata.st_mode) or not any(matches(p, relative) for p in spec['patterns']):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise RuntimeError('published output must be a regular file: ' + relative)
            if not spec.get('final', True) and time.time_ns() - max(metadata.st_mtime_ns, metadata.st_ctime_ns) < spec['settle_seconds'] * 1000000000:
                continue
            total += metadata.st_size
            if metadata.st_size > 1 << 30 or total > 4 << 30 or len(result) >= 256:
                raise RuntimeError('output size/file-count limit exceeded')
            result.append({'path': relative, 'size': metadata.st_size, 'version': version(metadata)})
    result.sort(key=lambda f: f['path'])
    if spec['kind'] == 'manifest':
        print(json.dumps(result))
    else:
        metrics = []
        for probe in spec['probes']:
            kind, settings = next(iter(probe.items()))
            if kind in ('jsonl_field', 'jsonl_series'):
                found = [(meta.st_mtime_ns, path) for path, meta in paths
                         if stat.S_ISREG(meta.st_mode) and matches(settings['file'], path, True)]
                points = 1 if kind == 'jsonl_field' else min(60 if settings.get('points') is None else settings['points'], 500)
                sample = values(os.path.join(root, max(found)[1]), settings['field'], points) if found else []
                metrics.append({'values': sample})
            elif kind == 'glob_count':
                metrics.append({'count': sum(matches(settings['pattern'], path, True) for path, _ in paths)})
            else:
                metrics.append(None)  # log regex is evaluated on the host's retained logs
        print(json.dumps({'files': result, 'metrics': metrics}, allow_nan=False))
else:
    raise RuntimeError('unknown job helper operation')
