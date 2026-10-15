#!/usr/bin/env python3
"""Isolated loopback lost-quorum recovery canary; only tiny native jobs, no installed-service writes."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--home', type=Path, required=True)
a = parser.parse_args()
a.binary = a.binary.resolve()
a.home.mkdir(parents=True, exist_ok=False)
children = {}
logs = []
result = {'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest()}


def run(*args, timeout=40):
    p = subprocess.run([str(a.binary), *map(str, args)], capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f'{args}: {p.stderr[-4000:]}')
    return json.loads(p.stdout) if p.stdout.strip() else None


def save(name, value):
    path = a.home / name
    path.write_text(json.dumps(value, indent=2))
    return path


def spawn(name, *args):
    log = (a.home / f'{name}.log').open('a')
    logs.append(log)
    children[name] = subprocess.Popen([str(a.binary), *map(str, args)], stdout=log, stderr=log)


def stop(name, kill=False):
    child = children[name]
    if child.poll() is None:
        child.kill() if kill else child.terminate()
        try:
            child.wait(timeout=12)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()


cluster = a.home / 'cluster'
worker = a.home / 'worker'
recovered = a.home / 'recovered'
key = cluster / 'client/node.key'
config = cluster / 'client/config.json'
creds = ['--key', key, '--config', config]


def read():
    return run('read', *creds)


def wait_for(predicate, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = read()
        if predicate(state):
            return state
        time.sleep(.15)
    raise RuntimeError(f'condition timed out: {state}')


try:
    run('init', '--home', cluster, '--replicas', 3)
    for i in (1, 2, 3):
        args = ['serve', '--home', cluster / f'node-{i}', '--config', cluster / f'node-{i}/config.json']
        spawn(f'node-{i}', *args, *(['--bootstrap'] if i == 1 else []))
    read()
    run('prepare-worker', '--home', worker, '--config', config, '--id', 'recovery-canary', '--cpu-slots', 1, '--memory-mb', 256)
    run('request', *creds, '--request', worker / 'enroll.json')
    spawn('worker', 'worker', '--home', worker)
    # Allow enrollment to apply to the future survivor before isolating it from later submissions.
    time.sleep(1)
    stop('node-1')
    job = save('job.json', {'name': 'recovery canary', 'argv': ['/bin/sh', '-c', 'echo started-once; echo recovered-output > result.txt; sleep 90'], 'threads': 1, 'keep_awake': False, 'max_seconds': 100, 'shutdown_grace_seconds': 0, 'settle_seconds': 0, 'publish': ['result.txt']})
    submit = ['submit-job', *creds, '--data-home', a.home / 'client-data', '--job', job, '--worker', 'recovery-canary', '--id', 'canary', '--request-id', 'submit-canary', '--memory-mb', 128]
    assert 'Accepted' in run(*submit)
    wait_for(lambda s: s['assignments']['canary']['observed'] and s['assignments']['canary']['observed'][1]['phase'] == 'Running')
    started = json.loads((worker / 'backend/jobs/exec-canary/supervisor.json').read_text())
    stop('worker', kill=True)
    stop('node-2', kill=True)
    stop('node-3', kill=True)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reservation:
        reservation.bind(('127.0.0.1', 0))
        address = f'127.0.0.1:{reservation.getsockname()[1]}'
    manifest = run('recover', '--source-home', cluster / 'node-1', '--config', cluster / 'node-1/config.json', '--key', key, '--home', recovered, '--bind', address, '--accept-data-loss', '--old-controllers-fenced')
    spawn('recovered', 'serve', '--home', recovered, '--config', recovered / 'config.json', '--bootstrap')
    run('accept-recovery', '--config', config, '--manifest', recovered / 'recovery.json')
    assert 'canary' not in read()['assignments']
    assert 'RecoveryUnknown' in run(*submit)
    run('rebind-worker', '--home', worker, '--manifest', recovered / 'recovery.json')
    spawn('worker-rebound', 'worker', '--home', worker)
    state = wait_for(lambda s: 'recovery-canary' in s['recovery']['inventories'])
    assert 'canary' in state['assignments']
    assert not state['recovery']['resumed']
    assert json.loads((worker / 'backend/jobs/exec-canary/supervisor.json').read_text()) == started
    origin = manifest['plan']['config']['cluster']
    assert run('resume-recovery', *creds, '--origin', origin, '--request-id', 'resume') == 'Recorded'
    cancel = save('cancel.json', {'origin': origin, 'id': 'cancel', 'mutation': {'Desire': {'id': 'canary', 'expected_revision': 1, 'desired': 'Cancelled'}}})
    assert 'Accepted' in run('request', *creds, '--request', cancel)
    wait_for(lambda s: s['assignments']['canary']['observed'][1]['phase'] == 'Cancelled')
    assert (worker / 'backend/jobs/exec-canary/stdout.log').read_text() == 'started-once\n'
    run('fetch', *creds, '--data-home', a.home / 'capture-data', '--assignment', 'canary', '--destination', a.home / 'outputs')
    assert (a.home / 'outputs/result.txt').read_text() == 'recovered-output\n'
    result.update({'lost_majority_recovered': True, 'unknown_retry_did_not_execute': True, 'missing_assignment_imported': True, 'same_supervisor_after_recovery': True, 'started_once': True, 'cancellation_observed': True, 'outputs_verified': True, 'manifest': manifest})
finally:
    # The canary has a strict time limit. If normal cancellation failed, only its own native
    # supervisor directory receives a cancel marker before fixture daemons are stopped.
    job_dir = worker / 'backend/jobs/exec-canary'
    if (job_dir / 'job.json').exists() and not (job_dir / 'exit.json').exists():
        (job_dir / 'cancel').write_text('recovery canary cleanup\n')
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not (job_dir / 'exit.json').exists():
            time.sleep(.1)
    for name in children:
        stop(name)
    for log in logs:
        log.close()
    result['fixture_processes_stopped'] = all(p.poll() is not None for p in children.values())
    result['canary_supervisor_finished'] = not (job_dir / 'job.json').exists() or (job_dir / 'exit.json').exists()
    save('qualification.json', result)
print(json.dumps(result, indent=2))
