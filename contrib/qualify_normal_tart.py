#!/usr/bin/env python3
"""Normal queue/service API with real offline Tart jobs, in a fresh isolated home."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

from control_host_fixture import preservation
from fleet import Fleet


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--home', type=Path, required=True)
    parser.add_argument('--instances', type=Path, required=True)
    parser.add_argument('--image-cache', type=Path, required=True)
    a = parser.parse_args()
    a.home.mkdir(parents=True, exist_ok=False)
    a.binary = a.binary.resolve()
    assert a.home.is_absolute() and a.home.resolve().is_relative_to(Path('/tmp').resolve())
    installed = Path.home() / '.fleet'
    for directory in (installed / 'jobs').glob('*'):
        assert not (directory / 'job.json').exists() or (directory / 'exit.json').exists(), 'legacy work is active'
    for record in (installed / 'instances').glob('*/observed.json'):
        assert json.loads(record.read_text())['state'] == 'deleted', 'legacy VM is active'
    before = preservation()
    result = {'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'network': 'offline', 'training_executed': False,
              'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest()}
    started = time.monotonic()
    child, logs = None, []
    client_home, worker = a.home / 'client', a.home / 'worker'

    def command(*args):
        p = subprocess.run([str(a.binary), *map(str, args)], capture_output=True, text=True, timeout=90)
        if p.returncode:
            raise RuntimeError(f'{args}: {p.stderr[-3000:]} {p.stdout[-1000:]}')
        return json.loads(p.stdout) if p.stdout.strip() else None

    def save(name, value):
        path = a.home / name
        path.write_text(json.dumps(value, indent=2))
        return path

    def start():
        log = (a.home / 'service.log').open('a')
        logs.append(log)
        return subprocess.Popen([str(a.binary), 'service', 'run', '--config', str(a.home / 'service.json')], stdout=log, stderr=log)

    def wait(call, predicate, seconds=160):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            last = call()
            if predicate(last):
                return last
            time.sleep(1)
        raise RuntimeError(f'timed out: {last}')

    def vm_phase(expected):
        return wait(lambda: client.instances(), lambda values: any(
            v['id'] == 'persistent' and v['observed'] and v['observed'][0] == v['revision'] and v['observed'][1]['phase'] == expected
            for v in values))

    command('cluster', 'init', '--home', a.home / 'seed', '--replicas', 1)
    command('--home', client_home, 'client', 'import', '--config', a.home / 'seed/client/config.json', '--key', a.home / 'seed/client/node.key')
    settings = json.loads(a.instances.read_text())
    settings.update(memory_budget_mb=8192, disk_budget_gb=100, min_free_disk_gb=10)
    command('cluster', 'prepare-worker', '--home', worker, '--config', client_home / 'config.json', '--id', 'tart-canary',
            '--cpu-slots', 4, '--memory-mb', 8192, '--instances', save('instances.json', settings))
    cache = worker / 'backend/instance-tart/cache'
    cache.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(['/bin/cp', '-cR', str(a.image_cache), str(cache)], check=True)
    command('service', 'configure', '--config', a.home / 'service.json', '--controller-home', a.home / 'seed/node-1',
            '--worker-home', worker, '--bootstrap')
    client = Fleet(client_home, a.binary)
    vm = dict(image='macos', cpus=2, memory_mb=4096, disk_gb=50, network='offline', gpu='exclusive')
    try:
        child = start()
        command('--home', client_home, 'enroll', worker / 'enroll.json', '--request-id', 'enroll')
        source = a.home / 'input.txt'
        source.write_text('normal queue verified input\n')
        program = "import json,time,pathlib; p=pathlib.Path('.'); print('guest-started-once',flush=True); (p/'result.txt').write_bytes((p/'input.txt').read_bytes());\nfor n in range(180):\n (p/'metrics.jsonl').open('a').write(json.dumps({'step':n})+'\\n'); time.sleep(1)"
        client.submit('Normal VM canary', ['/usr/bin/python3', '-c', program], worker='tart-canary',
                      inputs={'input.txt': source}, publish=['result.txt', 'metrics.jsonl'], memory_mb=4096,
                      keep_awake=False, settle_seconds=0, max_seconds=240, vm=vm, job_id='vm-canary', request_id='vm-submit',
                      probes=[{'jsonl_field': {'name': 'step', 'file': 'metrics.jsonl', 'field': 'step', 'total': 180}}])
        wait(lambda: client.job('vm-canary'), lambda v: v['phase'] == 'running')
        print('VM job running through normal queue', flush=True)
        native = client.submit('Alongside VM', ['/bin/sh', '-c', 'echo native-once'], worker='tart-canary', memory_mb=128,
                               keep_awake=False, settle_seconds=0, max_seconds=20, job_id='native-canary', request_id='native-submit')
        assert client.wait(native, timeout=60)['phase'] == 'succeeded'
        wait(lambda: client.outputs('vm-canary'), lambda files: any(v['path'] == 'result.txt' for v in files))
        client.fetch('vm-canary', a.home / 'live-output')
        assert (a.home / 'live-output/result.txt').read_bytes() == source.read_bytes()
        events = client.events('tart-canary')
        assert 'Progress' in json.dumps(events)
        save('live-events.json', events)
        result['live_verified_output_and_metrics'] = result['native_coexistence'] = True
        child.kill()
        child.wait(timeout=10)
        child = start()
        wait(lambda: client.job('vm-canary'), lambda v: v['phase'] == 'running')
        client.cancel('vm-canary', request_id='vm-cancel')
        assert client.wait('vm-canary', timeout=160)['phase'] == 'cancelled'
        assert client.logs('vm-canary')['data'].count('guest-started-once') == 1
        result['combined_service_restart_no_duplicate_guest_job'] = True
        print('VM job cancellation and combined-service restart passed', flush=True)
        spec = save('vm-spec.json', vm)
        command('--home', client_home, 'instance', 'create', 'persistent', '--worker', 'tart-canary', '--spec', spec, '--request-id', 'vm-create')
        vm_phase('Running')
        command('--home', client_home, 'instance', 'stop', 'persistent', '--request-id', 'vm-stop')
        vm_phase('Stopped')
        command('--home', client_home, 'instance', 'delete', 'persistent', '--request-id', 'vm-delete')
        vm_phase('Deleted')
        result['normal_persistent_vm_lifecycle'] = result['success'] = True
    except Exception as error:
        result.update(success=False, error=f'{type(error).__name__}: {error}')
        raise
    finally:
        if child is not None and child.poll() is not None:
            child = start()
        try:
            for job in client.queue():
                if job['phase'] not in ('succeeded', 'failed', 'cancelled'):
                    client.cancel(job['id'])
                    client.wait(job['id'], timeout=160)
            for vm_record in client.instances():
                if not vm_record['observed'] or vm_record['observed'][1]['phase'] != 'Deleted':
                    command('--home', client_home, 'instance', 'delete', vm_record['id'])
                    vm_phase('Deleted')
        finally:
            if child is not None:
                child.terminate()
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            for log in logs:
                log.close()
            infos = [json.loads(p.read_text()) for p in (worker / 'backend/instances').glob('*/observed.json')]
            result['all_fixture_vms_deleted'] = bool(infos) and all(i['state'] == 'deleted' for i in infos)
            result['legacy_records_unchanged'] = preservation() == before
            result['duration_seconds'] = round(time.monotonic() - started, 3)
            save('qualification.json', result)
            assert result['all_fixture_vms_deleted'] and result['legacy_records_unchanged'], 'cleanup or preservation failed'
            shutil.rmtree(cache)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
