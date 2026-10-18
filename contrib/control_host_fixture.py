#!/usr/bin/env python3
"""Local/SSH helper for an explicitly isolated multi-host control-plane qualification."""
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import time

CHILDREN = {}


def validate_service(root, command):
    assert '--config' in command, 'service configuration required'
    config_path = Path(command[command.index('--config') + 1]).resolve()
    assert config_path.is_relative_to(root.resolve()), 'service configuration escapes fixture'
    config = json.loads(config_path.read_text())
    homes = [config.get(k) for k in ('controller_home', 'worker_home', 'storage_home', 'client_home')]
    assert any(homes), 'service has no roles'
    assert all(Path(h).is_absolute() and Path(h).resolve().is_relative_to(root.resolve())
               for h in homes if h), 'service role escapes fixture'


def process_identity(pid):
    child = CHILDREN.get(pid)
    if child is not None and child.poll() is not None:
        CHILDREN.pop(pid)
        return None
    p = subprocess.run(['ps', '-p', str(pid), '-o', 'stat=', '-o', 'lstart=', '-o', 'command='],
                       capture_output=True, text=True)
    fields = p.stdout.strip().split(None, 1)
    # A held launch session may briefly retain a zombie until it reaps its child.
    return fields[1].strip() if p.returncode == 0 and len(fields) == 2 and not fields[0].startswith('Z') else None


def preservation():
    home = Path.home() / '.fleet'
    records = {}
    for kind, names in [('jobs', ('job.json', 'supervisor.json', 'exit.json')),
                        ('instances', ('desired.json', 'observed.json'))]:
        for directory in (home / kind).glob('*'):
            for name in names:
                p = directory / name
                if p.is_file():
                    records[str(p.relative_to(home))] = hashlib.sha256(p.read_bytes()).hexdigest()
    database = home / 'fleet.db'
    if database.exists():
        with sqlite3.connect(f'file:{database}?mode=ro', uri=True) as db:
            records['queue_rows'] = db.execute('select * from jobs order by id').fetchall()
    binary = Path.home() / 'fleet/bin/fleet'
    if binary.is_file():
        records['installed_binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        p = subprocess.run(['ps', '-axo', 'pid=,command='], capture_output=True, text=True, check=True)
        records['installed_services'] = [line.strip() for line in p.stdout.splitlines()
                                         if str(binary) + ' agent' in line or str(binary) + ' daemon' in line]
    return records


def main(request):
    root = Path(request['root']).resolve()
    assert root.is_relative_to(Path('/tmp').resolve()), 'fixture home must be under /tmp'
    assert any(p.startswith('fleet-control-') for p in root.parts), 'fixture prefix required'
    assert root != Path('/tmp').resolve()
    op = request['op']
    if op == 'create':
        root.mkdir(parents=True, mode=0o700, exist_ok=False)
        (root / '.fleet-qualification').write_text('isolated control-plane fixture\n')
        return {'hostname': socket.gethostname(), 'system': platform.system(), 'architecture': platform.machine(),
                'preservation': preservation()}
    assert (root / '.fleet-qualification').is_file(), 'not an initialized fixture home'

    def path(relative):
        p = (root / relative).resolve()
        assert p.is_relative_to(root) and p != root, 'path escapes fixture home'
        return p

    if op == 'preservation':
        return preservation()
    if op == 'write':
        p = path(request['path'])
        p.parent.mkdir(parents=True, exist_ok=True)
        temp = p.with_name(p.name + '.fixture-tmp')
        temp.write_bytes(base64.b64decode(request['data']))
        temp.chmod(0o600)
        temp.replace(p)
        return True
    if op == 'read':
        p = path(request['path'])
        return base64.b64encode(p.read_bytes()).decode() if p.exists() else None
    if op == 'port':
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((request['address'], request['port']))
        return True
    name = request['name']
    assert re.fullmatch('[a-z0-9-]+', name)
    marker = path(f'processes/{name}.json')
    if op == 'spawn':
        if marker.exists():
            old = json.loads(marker.read_text())
            assert process_identity(old['pid']) is None, 'fixture process already exists'
        command = request['command']
        assert Path(command[0]).is_absolute(), 'explicit executable required'
        if 'service' in command and 'run' in command:
            validate_service(root, command)
        else:
            assert '--home' in command and Path(command[command.index('--home') + 1]).resolve().is_relative_to(root)
        marker.parent.mkdir(exist_ok=True)
        with path(f'{name}.log').open('ab') as log:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                     cwd=root, start_new_session=True)
        time.sleep(.1)
        assert child.poll() is None, f'fixture process exited: {name}'
        CHILDREN[child.pid] = child
        record = {'pid': child.pid, 'identity': process_identity(child.pid), 'command': command}
        marker.write_text(json.dumps(record))
        return record
    record = json.loads(marker.read_text())
    actual = process_identity(record['pid'])
    if actual is None:
        return False
    assert actual == record['identity'], 'PID identity changed; refusing to signal it'
    if op == 'alive':
        return True
    if op == 'stop':
        os.kill(record['pid'], signal.SIGCONT)
        os.kill(record['pid'], signal.SIGKILL if request.get('kill') else signal.SIGTERM)
        deadline = time.monotonic() + 10
        while process_identity(record['pid']) is not None and time.monotonic() < deadline:
            time.sleep(.1)
        actual = process_identity(record['pid'])
        if actual is not None:
            assert actual == record['identity'], 'PID identity changed during stop'
            os.kill(record['pid'], signal.SIGKILL)
            time.sleep(.3)
        assert process_identity(record['pid']) is None, 'fixture process did not exit'
        return True
    raise ValueError(f'unknown operation {op}')


if __name__ == '__main__':
    request = json.load(sys.stdin)
    result = main(request)
    print(json.dumps(result), flush=True)
    if request.get('hold_session'):
        assert request['op'] == 'spawn'
        # Keep the authorized SSH ancestor alive. Orphaned macOS processes can lose Local
        # Network access when that session ends. This is not a tunnel or a permission change.
        CHILDREN[result['pid']].wait()
