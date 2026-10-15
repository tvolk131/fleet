#!/usr/bin/env python3
"""Checkpoint lifecycle through the normal client, with fake or real offline Tart.

Always uses fresh homes under /tmp. Real mode requires --instances and --image-cache.
Optional --job supplies application-specific Fleet.submit keyword arguments; its
first attempt must publish a checkpoint and remain running until suspension.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

from control_host_fixture import preservation
from fleet import Fleet, FleetError

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--home', type=Path, required=True)
    parser.add_argument('--fake', action='store_true')
    parser.add_argument('--instances', type=Path)
    parser.add_argument('--image-cache', type=Path)
    parser.add_argument('--job', type=Path)
    a = parser.parse_args()
    assert a.home.is_absolute() and a.home.resolve().is_relative_to(Path('/tmp').resolve())
    assert a.home.name.startswith('fleet-control-'), 'isolated fixture prefix required'
    assert a.fake or (a.instances and a.image_cache)
    a.home.mkdir(mode=0o700, parents=True, exist_ok=False)
    # Supervisors and restarted services must use one immutable candidate, even while a developer rebuilds.
    shutil.copy2(a.binary.resolve(), a.home / 'fleet')
    a.binary = a.home / 'fleet'
    before = preservation()
    installed = Path.home() / 'fleet/replicated/service.json'
    normal_before = None
    if not a.fake and installed.exists():
        # Read through the installed version; never open its databases with the fixture binary.
        config = json.loads(installed.read_text())
        normal = Fleet(config['client_home'], Path.home() / 'fleet/bin/fleet')
        normal_before = {'jobs': normal.queue(), 'instances': normal.instances()}
        assert all(j['phase'] in ('succeeded', 'failed', 'cancelled', 'suspended')
                   for j in normal_before['jobs']), 'installed Fleet has active jobs'
        assert all(i['observed'] and i['observed'][1]['phase'] in ('Stopped', 'Deleted', {'Instance': 'Stopped'}, {'Instance': 'Deleted'})
                   for i in normal_before['instances']), 'installed Fleet has active VMs'
    if not a.fake:
        for p in (Path.home() / '.fleet/jobs').glob('*/job.json'):
            assert (p.parent / 'exit.json').exists(), 'legacy job active'
    result = {'real_tart': not a.fake, 'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest(), 'read_retries': 0}
    worker, client_home = a.home / 'worker', a.home / 'client'
    child, logs, cache = None, [], None

    def save(name, value):
        p = a.home / name
        p.write_text(json.dumps(value, indent=2))
        return p

    def command(*args):
        p = subprocess.run([str(a.binary), *map(str, args)], text=True, capture_output=True, timeout=90)
        if p.returncode:
            raise RuntimeError(f'{args}: {p.stderr[:2000]} {p.stdout[-1000:]}')
        return json.loads(p.stdout) if p.stdout.strip() else None

    def start():
        log = (a.home / 'service.log').open('a')
        logs.append(log)
        return subprocess.Popen([str(a.binary), 'service', 'run', '--config', str(a.home / 'service.json')], stdout=log, stderr=log)

    def wait(call, predicate, seconds=240):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            try:
                last = call()
                if predicate(last):
                    return last
            except FleetError as error:
                # These call sites only poll reads. Retry transport deadlines within the overall
                # bound, and retain the count as qualification evidence. Semantic errors still fail.
                if 'control-plane request deadline exceeded' not in str(error):
                    raise
                last = str(error)
                result['read_retries'] += 1
                if child is not None and child.poll() is not None:
                    raise RuntimeError('fixture service exited during read polling') from error
            time.sleep(.5)
        raise RuntimeError(f'timed out: {last}')

    command('cluster', 'init', '--home', a.home / 'seed', '--replicas', 1)
    command('--home', client_home, 'client', 'import', '--config', a.home / 'seed/client/config.json', '--key', a.home / 'seed/client/node.key')
    if a.fake:
        tart = a.home / 'tart'
        shutil.copyfile(ROOT / 'tests/fixtures/fake_tart.py', tart)
        tart.chmod(0o700)
        softnet = a.home / 'softnet'
        softnet.write_text("#!/bin/sh\necho 'softnet 0.24.0-test'\n")
        softnet.chmod(0o700)
        settings = {'tart': str(tart), 'softnet': str(softnet), 'images': {'macos': 'registry.test/macos@sha256:' + 'a' * 64}, 'block_cidrs': []}
    else:
        settings = json.loads(a.instances.read_text())
    # Fleet reserves host capacity for the OS. Hosted CI runners can expose only one usable slot;
    # the fake VM needs no real guest RAM and still obeys Fleet's 4 GiB VM admission minimum.
    cpu_slots, memory_mb = (1, 4096) if a.fake else (4, 8192)
    vm_cpus, vm_memory = (1, 4096) if a.fake else (2, 6144)
    settings.update(memory_budget_mb=memory_mb, disk_budget_gb=100, min_free_disk_gb=5)
    command('cluster', 'prepare-worker', '--home', worker, '--config', client_home / 'config.json', '--id', 'checkpoint-worker',
            '--cpu-slots', cpu_slots, '--memory-mb', memory_mb, '--instances', save('instances.json', settings))
    if not a.fake:
        cache = worker / 'backend/instance-tart/cache'
        cache.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(['/bin/cp', '-cR', str(a.image_cache), str(cache)], check=True)
    command('service', 'configure', '--config', a.home / 'service.json', '--controller-home', a.home / 'seed/node-1', '--worker-home', worker, '--bootstrap')
    client = Fleet(client_home, a.binary)
    first = "import hashlib,json,pathlib,time; p=pathlib.Path('checkpoints/one'); p.mkdir(parents=True); (p/'state').write_text('7'); m={'version':1,'position':7,'files':{'state':{'size':1,'sha256':hashlib.sha256(b'7').hexdigest()}}}; (p/'ready.tmp').write_text(json.dumps(m)); (p/'ready.tmp').rename(p/'fleet.json'); print('initial-once',flush=True); time.sleep(180)"
    resume = "import pathlib,sys; n=int(pathlib.Path(sys.argv[1]).read_text()); pathlib.Path('result.txt').write_text(str(n+1)); print('resumed-once',flush=True)"
    job = json.loads(a.job.read_text()) if a.job else dict(
        name='Checkpoint qualification', argv=['/usr/bin/python3', '-c', first], publish=['result.txt'],
        checkpoint={'manifests': 'checkpoints/*/fleet.json', 'resume_argv': ['/usr/bin/python3', '-c', resume, '{checkpoint}/state']})
    job.update(worker='checkpoint-worker', keep_awake=False, settle_seconds=0, max_seconds=600,
               job_id='resumable', request_id='submit', threads=vm_cpus,
               vm=dict(image='macos', cpus=vm_cpus, memory_mb=vm_memory, disk_gb=50, network='offline', gpu='exclusive'))
    try:
        child = start()
        command('--home', client_home, 'enroll', worker / 'enroll.json', '--request-id', 'enroll')
        client.submit(**job)
        cp = wait(lambda: client.checkpoints('resumable'), bool)[0]
        print('Verified checkpoint published', flush=True)
        client.fetch_checkpoint(cp['id'], a.home / 'checkpoint-download')
        assert (a.home / 'checkpoint-download/checkpoint.json').is_file()
        client.suspend('resumable', checkpoint=cp['id'], request_id='suspend')
        assert client.retry('suspend')['reply']['Accepted']['job'] == 'resumable'
        wait(lambda: client.job('resumable'), lambda j: j['phase'] == 'suspended')
        attempts = client.attempts('resumable')
        assert len(attempts) == 1
        assert all(json.loads(p.read_text())['state'] == 'deleted' for p in (worker / 'backend/instances').glob('*/observed.json'))
        child.kill()
        child.wait(timeout=10)
        child = start()
        wait(lambda: client.job('resumable'), lambda j: j['phase'] == 'suspended')
        command('--home', client_home, 'resume', 'resumable', '--request-id', 'resume')
        client.retry('resume')
        final = wait(lambda: client.job('resumable'), lambda j: j['phase'] in ('succeeded', 'failed', 'cancelled'), seconds=300)
        assert final['phase'] == 'succeeded', final
        attempts = client.attempts('resumable')
        assert len(attempts) == 2 and attempts[0]['execution'] != attempts[1]['execution']
        for n in (1, 2):
            command('--home', client_home, 'logs', 'resumable', '--attempt', n)
            save(f'attempt-{n}-logs.json', client.logs('resumable', attempt=n))
        client.fetch('resumable', a.home / 'outputs', attempt=2)
        if not a.job:
            assert (a.home / 'outputs/result.txt').read_text() == '8'
            assert client.logs('resumable', attempt=1)['data'].count('initial-once') == 1
            assert client.logs('resumable', attempt=2)['data'].count('resumed-once') == 1
        result.update(success=True, checkpoint_export_verified=True, vm_deleted_before_resume=True,
                      combined_service_restart=True, unique_attempts=2, stable_job_identity=True)
        print('Suspension, restart, fresh-VM resume, and historical logs passed', flush=True)
    except Exception as error:
        result.update(success=False, error=str(error))
        service_log = a.home / 'service.log'
        if service_log.exists():
            diagnostics = service_log.read_text()
            print('Fixture service diagnostics:\n' + diagnostics[:2000] + '\n...\n' + diagnostics[-2000:], flush=True)
        raise
    finally:
        if child is not None:
            if child.poll() is not None:
                child = start()
            try:
                for j in client.queue():
                    if j['phase'] not in ('succeeded', 'failed', 'cancelled'):
                        client.cancel(j['id'])
                        client.wait(j['id'], timeout=180)
            except Exception as cleanup_error:
                result['cleanup_client_error'] = str(cleanup_error)
                # A broken client must not leave this fixture's detached jobs running. Never touch
                # records outside this freshly created worker home.
                for record in (worker / 'backend/jobs').glob('*/job.json'):
                    if not (record.parent / 'exit.json').exists():
                        (record.parent / 'cancel').write_text('isolated qualification cleanup\n')
                wait(lambda: list((worker / 'backend/jobs').glob('*/job.json')),
                     lambda records: all((r.parent / 'exit.json').exists() for r in records), seconds=180)
            finally:
                child.terminate()
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=10)
        for log in logs:
            log.close()
        states = [json.loads(p.read_text())['state'] for p in (worker / 'backend/instances').glob('*/observed.json')]
        result['fixture_vms_created'] = len(states)
        result['all_fixture_vms_deleted'] = all(s == 'deleted' for s in states)
        result['legacy_records_unchanged'] = preservation() == before
        if normal_before is not None:
            result['normal_records_unchanged'] = normal_before == {'jobs': normal.queue(), 'instances': normal.instances()}
        save('qualification.json', result)
        assert result['all_fixture_vms_deleted'] and result['legacy_records_unchanged'] and result.get('normal_records_unchanged', True)
        if cache:
            shutil.rmtree(cache)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
