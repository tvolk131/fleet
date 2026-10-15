#!/usr/bin/env python3
"""Opt-in real-Tart canary. Explicit empty home, loopback cluster, no installed-service writes."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--home', type=Path, required=True)
p.add_argument('--instances', type=Path, required=True)
p.add_argument('--image-cache', type=Path, required=True)
p.add_argument('--installed-home', type=Path, required=True)
a = p.parse_args()
a.binary = a.binary.resolve()
a.home.mkdir(parents=True, exist_ok=True)
assert not list(a.home.iterdir()), 'qualification home must be empty'
assert a.home.resolve() != a.installed_home.resolve()


def preservation():
    result = {}
    for kind, names in [('jobs', ('job.json', 'exit.json', 'supervisor.json')), ('instances', ('desired.json', 'observed.json'))]:
        for d in (a.installed_home / kind).iterdir():
            for name in names:
                path = d / name
                if path.exists():
                    result[str(path.relative_to(a.installed_home))] = hashlib.sha256(path.read_bytes()).hexdigest()
    with sqlite3.connect(f'file:{a.installed_home}/fleet.db?mode=ro', uri=True) as db:
        # Queue contents are read only; no reservation or canary is added to the legacy system.
        result['queue_rows'] = db.execute('select * from jobs order by id').fetchall()
    return result


for d in (a.installed_home / 'jobs').iterdir():
    assert not (d / 'job.json').exists() or (d / 'exit.json').exists(), f'unfinished legacy job: {d.name}'
for d in (a.installed_home / 'instances').iterdir():
    if (d / 'desired.json').exists():
        assert json.loads((d / 'observed.json').read_text())['state'] == 'deleted', f'legacy VM still present: {d.name}'
before = preservation()
(a.home / 'legacy-before.json').write_text(json.dumps(before))


def run(*args, timeout=45):
    out = subprocess.run([str(a.binary), *map(str, args)], capture_output=True, text=True, timeout=timeout)
    if out.returncode:
        raise RuntimeError(f'{args}: {out.stderr[-4000:]}')
    return json.loads(out.stdout) if out.stdout.strip() else None


def save(name, value):
    path = a.home / name
    path.write_text(json.dumps(value, indent=2))
    return path


run('init', '--home', a.home / 'cluster', '--replicas', 3)
cluster = a.home / 'cluster'
key = cluster / 'client/node.key'
config = cluster / 'client/config.json'
creds = ['--key', key, '--config', config]
children = {}
logs = []


def spawn(name, *args):
    log = (a.home / f'{name}.log').open('a')
    logs.append(log)
    child = subprocess.Popen([str(a.binary), *map(str, args)], stdout=log, stderr=log)
    children[name] = child
    return child


def replica(i, bootstrap=False):
    args = ['serve', '--home', cluster / f'node-{i}', '--config', cluster / f'node-{i}/config.json']
    if bootstrap:
        args += ['--bootstrap']
    return spawn(f'replica-{i}', *args)


def read():
    return run('read', *creds)


def request(name, mutation):
    reply = run('request', *creds, '--request', save(f'{name}.json', {'origin': json.loads(config.read_text())['cluster'], 'id': name, 'mutation': mutation}))
    assert 'Rejected' not in reply, reply
    return reply


def data(name, payload):
    return run('data', *creds, '--worker', 'canary', '--request', save(f'data-{name}.json', payload))


def wait_phase(name, phase, seconds=150):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        state = read()
        item = state['assignments'].get(name)
        if item and item['observed'] and item['observed'][0] == item['revision'] and item['observed'][1]['phase'] == phase:
            print(f'{name}: {phase}', flush=True)
            return item
        time.sleep(.5)
    raise RuntimeError(f'{name} did not reach {phase}: {item}')


worker_home = a.home / 'worker'
vm = {'image': 'macos', 'cpus': 2, 'memory_mb': 4096, 'disk_gb': 50, 'network': 'offline', 'gpu': 'exclusive'}
result = {'scope': 'replicated real worker canary', 'network': 'offline', 'training_executed': False, 'home': str(a.home)}
try:
    for i in (1, 2, 3):
        replica(i, i == 1)
    read()
    settings = json.loads(a.instances.read_text())
    settings.update(memory_budget_mb=8192, disk_budget_gb=100, min_free_disk_gb=10)
    instance_config = save('instances.json', settings)
    run('prepare-worker', '--home', worker_home, '--config', config, '--id', 'canary', '--cpu-slots', 4, '--memory-mb', 8192, '--instances', instance_config)
    tart_home = worker_home / 'backend/instance-tart'
    tart_home.mkdir(parents=True)
    # APFS clone: independent writable cache; never symlink back into the installed worker.
    subprocess.run(['/bin/cp', '-cR', str(a.image_cache), str(tart_home / 'cache')], check=True)
    run('request', *creds, '--request', worker_home / 'enroll.json')
    spawn('worker', 'worker', '--home', worker_home)
    source = save('input.json', {'value': 'verified input'})
    program = "import json,time,pathlib; p=pathlib.Path('.'); print('started-once',flush=True); value=json.loads((p/'input.json').read_text())['value']; (p/'result.txt').write_text(value);\nfor n in range(120):\n (p/'metrics.jsonl').open('a').write(json.dumps({'step':n})+'\\n'); time.sleep(1)"
    job = {'name': 'replicated real Tart canary', 'argv': ['/usr/bin/python3', '-c', program], 'inputs': {'input.json': str(source)}, 'publish': ['result.txt', 'metrics.jsonl'], 'settle_seconds': 0, 'threads': 1, 'keep_awake': False, 'max_seconds': 180, 'probes': [{'jsonl_field': {'name': 'step', 'file': 'metrics.jsonl', 'field': 'step', 'total': 120}}], 'vm': vm}
    run('submit-job', *creds, '--data-home', a.home / 'client-data', '--job', save('job.json', job), '--worker', 'canary', '--id', 'vm-canary', '--request-id', 'vm-canary-submit', '--memory-mb', 4096, timeout=90)
    wait_phase('vm-canary', 'Running')
    # Native job alongside the VM shares Fleet's declared CPU/memory admission.
    native = {'name': 'replicated native canary', 'argv': ['/bin/sh', '-c', 'echo native-once; echo native-result > native.txt'], 'publish': ['native.txt'], 'threads': 1, 'keep_awake': False, 'max_seconds': 10}
    run('submit-job', *creds, '--data-home', a.home / 'client-data', '--job', save('native.json', native), '--worker', 'canary', '--id', 'native-canary', '--request-id', 'native-canary-submit', '--memory-mb', 128)
    wait_phase('native-canary', 'Succeeded')
    end = time.monotonic() + 90
    while time.monotonic() < end:
        offered = data('outputs', {'Outputs': {'execution': 'exec-vm-canary'}})['Outputs']
        events = data('events', {'Events': {'since': 0, 'wait_ms': 0}})
        if any(f['path'] == 'result.txt' for f in offered) and 'Progress' in json.dumps(events):
            break
        time.sleep(1)
    else:
        raise RuntimeError('no live VM outputs/progress')
    save('live-events.json', events)
    run('fetch', *creds, '--data-home', a.home / 'client-data', '--assignment', 'vm-canary', '--destination', a.home / 'live-capture')
    assert (a.home / 'live-capture/result.txt').read_text() == 'verified input'
    result['live_output_verified'] = result['live_metrics'] = True
    # The latest committed worker report identifies the leader that accepted it.
    leader = read()['last_applied']['leader_id']['node_id']
    result['killed_leader'] = leader
    children[f'replica-{leader}'].kill(); children[f'replica-{leader}'].wait()
    children['worker'].kill(); children['worker'].wait()
    read()  # Must succeed with the original leader absent, before restarting that replica.
    spawn('worker', 'serve', '--home', cluster / f'node-{leader}', '--config', cluster / f'node-{leader}/config.json', '--worker-home', worker_home)
    result['combined_mode_recovery'] = True
    wait_phase('vm-canary', 'Running')
    request('cancel-vm', {'Desire': {'id': 'vm-canary', 'expected_revision': 1, 'desired': 'Cancelled'}})
    wait_phase('vm-canary', 'Cancelled')
    final_logs = data('logs', {'Logs': {'execution': 'exec-vm-canary', 'stderr': False, 'from': 0, 'max': 524288}})
    assert final_logs['Logs']['data'].count('started-once') == 1, final_logs
    result['worker_sigkill_recovered'] = result['replica_loss_recovered'] = result['guest_started_once'] = True
    # Persistent instance lifecycle uses the same replicated worker path.
    request('create-instance', {'Assign': {'id': 'persistent', 'execution': 'exec-persistent', 'worker': 'canary', 'workload': {'Instance': {'spec': vm}}}})
    wait_phase('persistent', 'Running')
    request('stop-instance', {'Desire': {'id': 'persistent', 'expected_revision': 1, 'desired': 'Stopped'}})
    wait_phase('persistent', 'Stopped')
    request('delete-instance', {'Desire': {'id': 'persistent', 'expected_revision': 2, 'desired': 'Deleted'}})
    wait_phase('persistent', 'Deleted')
    result['persistent_instance_lifecycle'] = True
    result['state'] = read()
finally:
    # Restore only fixture processes if needed to deliver committed cleanup requests.
    if 'worker' in children and children['worker'].poll() is not None:
        spawn('worker', 'worker', '--home', worker_home)
    try:
        state = read()
        for name, item in state['assignments'].items():
            phase = item['observed'][1]['phase'] if item['observed'] else None
            target = 'Deleted' if 'Instance' in item['workload'] else 'Cancelled'
            if phase not in ('Deleted', 'Succeeded', 'Failed', 'Cancelled'):
                if item['desired'] != target:
                    request(f'cleanup-{name}', {'Desire': {'id': name, 'expected_revision': item['revision'], 'desired': target}})
                wait_phase(name, target)
    finally:
        for child in children.values():
            if child.poll() is None:
                child.terminate()
        for child in children.values():
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait()
        for log in logs:
            log.close()
        after = preservation()
        result['legacy_records_unchanged'] = before == after
        result['fixture_processes_stopped'] = all(p.poll() is not None for p in children.values())
        infos = [json.loads(f.read_text()) for f in (worker_home / 'backend/instances').glob('*/observed.json')]
        result['all_fixture_vms_deleted'] = bool(infos) and all(i['state'] == 'deleted' for i in infos)
        save('qualification.json', result)
        assert result['legacy_records_unchanged'], 'installed records changed during canary'
        assert result['all_fixture_vms_deleted'], 'fixture VM cleanup incomplete'
        if result['all_fixture_vms_deleted']:
            shutil.rmtree(worker_home / 'backend/instance-tart/cache', ignore_errors=True)
print(json.dumps({k: v for k, v in result.items() if k != 'state'}, indent=2))
