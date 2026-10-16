#!/usr/bin/env python3
"""Portable Tart jobs through the JSON CLI, in fresh homes only. Never upgrades installed Fleet."""
import argparse
from functools import partial
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

from cli_fixture import api, api_result
from control_host_fixture import preservation

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--home', type=Path, required=True)
    p.add_argument('--fake', action='store_true')
    p.add_argument('--instances', type=Path)
    p.add_argument('--image-cache', type=Path)
    a = p.parse_args()
    assert a.home.is_absolute() and a.home.resolve().is_relative_to(Path('/tmp').resolve())
    assert a.home.name.startswith('fleet-control-') and not a.home.exists()
    assert a.fake or (a.instances and a.image_cache)
    before = preservation()
    normal, normal_before = None, None
    installed = Path.home() / 'fleet/replicated/service.json'
    if not a.fake:
        if installed.exists():
            config = json.loads(installed.read_text())
            normal = partial(api_result, Path.home() / 'fleet/bin/fleet', config['client_home'])
            normal_before = dict(jobs=normal(op='queue'), instances=normal(op='instances'))
            assert all(j['phase'] in ('succeeded', 'failed', 'cancelled', 'suspended') for j in normal_before['jobs']), 'installed jobs active'
            assert all(i['observed'] and i['observed'][1]['phase'] in ('Stopped', 'Deleted', {'Instance': 'Stopped'}, {'Instance': 'Deleted'}) for i in normal_before['instances']), 'installed VMs active'
        assert all((f.parent / 'exit.json').exists() for f in (Path.home() / '.fleet/jobs').glob('*/job.json')), 'legacy training active'
    a.home.mkdir(mode=0o700, parents=True)
    shutil.copy2(a.binary.resolve(), a.home / 'fleet')
    binary = a.home / 'fleet'
    result = dict(real_tart=not a.fake, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), physical_failure_domain='same-machine; no physical host-loss claim')
    processes, logs, caches = {}, [], []
    client = a.home / 'client'
    source, destination, storage = [a.home / n for n in ('source', 'destination', 'storage')]

    def save(name, value):
        path = a.home / name
        path.write_text(json.dumps(value, indent=2))
        return path

    def command(*args):
        output = subprocess.run([str(binary), *map(str, args)], capture_output=True, text=True, timeout=120)
        if output.returncode:
            raise RuntimeError(f'{args}: {output.stderr[:2000]} {output.stdout[-1000:]}')
        return json.loads(output.stdout) if output.stdout.strip() else None

    def request(**args):
        return api_result(binary, client, **args)

    def start(name):
        log = (a.home / f'{name}.log').open('a')
        logs.append(log)
        processes[name] = subprocess.Popen([str(binary), 'service', 'run', '--config', str(a.home / f'{name}.json')], stdout=log, stderr=log)

    def stop(name):
        child = processes.pop(name, None)
        if child:
            child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)

    def wait(call, predicate, seconds=240):
        end, last = time.monotonic() + seconds, None
        while time.monotonic() < end:
            last = call()
            if predicate(last):
                return last
            time.sleep(.5)
        raise RuntimeError(f'timed out: {last}')

    command('cluster', 'init', '--home', a.home / 'seed', '--replicas', 1)

    def import_client():
        command('--home', client, 'client', 'import', '--config', a.home / 'seed/client/config.json', '--key', a.home / 'seed/client/node.key')

    import_client()
    if a.fake:
        tart, softnet = a.home / 'tart', a.home / 'softnet'
        shutil.copyfile(ROOT / 'tests/fixtures/fake_tart.py', tart)
        softnet.write_text("#!/bin/sh\necho 'softnet 0.24.0-test'\n")
        tart.chmod(0o700)
        softnet.chmod(0o700)
        settings = dict(tart=str(tart), softnet=str(softnet), images={'macos': 'registry.test/macos@sha256:' + 'a' * 64}, block_cidrs=[])
    else:
        settings = json.loads(a.instances.read_text())
    platform = dict(os='macos', arch='aarch64', capabilities=[])
    settings.update(platforms={'macos': platform}, memory_budget_mb=4096 if a.fake else 8192, disk_budget_gb=100, min_free_disk_gb=5)
    runtime = dict(platform=platform, image_digest=settings['images']['macos'].split('@')[1])
    for name, home in [('source', source), ('destination', destination)]:
        initial_settings = {**settings, 'platforms': {} if name == 'destination' else settings['platforms']}
        command('cluster', 'prepare-worker', '--home', home, '--config', client / 'config.json', '--id', name,
                '--cpu-slots', 1 if a.fake else 4, '--memory-mb', settings['memory_budget_mb'], '--instances', save('instances.json', initial_settings))
        if not a.fake:
            cache = home / 'backend/instance-tart/cache'
            cache.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(['/bin/cp', '-cR', str(a.image_cache), str(cache)], check=True)
            caches.append(cache)
        command('service', 'configure', '--config', a.home / f'{name}.json', '--worker-home', home)
    command('cluster', 'prepare-storage', '--home', storage, '--config', client / 'config.json', '--id', 'backup', '--failure-domain', 'same-qualification-machine', '--capacity-bytes', 32 << 20)
    command('service', 'configure', '--config', a.home / 'control.json', '--controller-home', a.home / 'seed/node-1', '--storage-home', storage, '--bootstrap')
    inputs = a.home / 'original-inputs'
    inputs.mkdir()
    trainer = inputs / 'trainer'
    trainer.write_text('''#!/usr/bin/python3
import hashlib,json,pathlib,sys,time
assert pathlib.Path('dataset').read_text() == 'durable original data'
if sys.argv[1]=='resume':
 n=int(pathlib.Path(sys.argv[2]).read_text())
 pathlib.Path('result.txt').write_text(str(n+1))
 print('resumed-once',flush=True)
else:
 p=pathlib.Path('cp/one');p.mkdir(parents=True)
 (p/'state').write_text('7')
 m={'version':1,'position':7,'files':{'state':{'size':1,'sha256':hashlib.sha256(b'7').hexdigest()}}}
 (p/'ready.tmp').write_text(json.dumps(m));(p/'ready.tmp').rename(p/'fleet.json')
 print('initial-once',flush=True);time.sleep(500)
''')
    trainer.chmod(0o700)
    (inputs / 'dataset').write_text('durable original data')
    job = dict(name='Portable qualification', argv=['bin/trainer', 'start'], inputs={'bin/trainer': str(trainer), 'dataset': str(inputs / 'dataset')},
               publish=['result.txt'], keep_awake=False, settle_seconds=0, max_seconds=600, threads=1 if a.fake else 2,
               vm=dict(image='macos', runtime=runtime, cpus=1 if a.fake else 2, memory_mb=4096 if a.fake else 6144, disk_gb=50, network='offline', gpu='exclusive'),
               checkpoint=dict(manifests='cp/*/fleet.json', resume_argv=['bin/trainer', 'resume', '{checkpoint}/state']))
    try:
        start('control')
        for home in (source, destination):
            command('--home', client, 'enroll', home / 'enroll.json', '--request-id', 'enroll-' + home.name)
        command('--home', client, 'storage', 'enroll', storage / 'registration.json', '--request-id', 'enroll-backup')
        command('--home', client, 'storage', 'policy', '--revision', 0, '--input', 'backup', '--checkpoint', 'backup', '--output', 'backup', '--request-id', 'policy')
        request(op='drain', worker='destination', drain=True, request_id='drain-destination')
        # Enable portable execution on an already enrolled worker without a new identity/home.
        old_registration = json.loads((destination / 'worker.json').read_text())['registration']
        (destination / 'backend/instances.json').write_text(json.dumps(settings))
        prepared = command('cluster', 'prepare-worker-runtimes', '--home', destination)
        assert command('cluster', 'prepare-worker-runtimes', '--home', destination) == prepared
        for _ in range(2):
            assert command('cluster', 'request', '--key', client / 'node.key', '--config', client / 'config.json',
                           '--request', destination / 'runtimes-request.json') == 'Recorded'
        updated = json.loads((destination / 'worker.json').read_text())['registration']
        assert updated.pop('runtimes') == {'macos': runtime}
        old_registration.pop('runtimes', None)
        assert updated == old_registration
        result['existing_worker_runtime_update'] = True
        start('source')
        start('destination')
        request(op='submit', id='portable', request_id='portable-submit', memory_mb=0, job=job)

        def stage_and_read():
            command('--home', client, 'stage', 'portable')
            return request(op='job', id='portable')

        initial = wait(stage_and_read, lambda j: j['latest_checkpoint'] is not None)
        checkpoint = initial['latest_checkpoint']
        assert initial['assignment']['worker'] == 'source'
        bundle = initial['input_bundle']
        wait(lambda: request(op='artifacts'), lambda rows: any(r['artifact']['manifest']['kind'] == {'Checkpoint': checkpoint['id']} and r['artifact']['copies']['storage/backup']['phase'] == 'Ready' for r in rows))
        save('before-suspend.json', request(op='job', id='portable'))
        request(op='suspend', id='portable', checkpoint=checkpoint['id'], expected_revision=initial['revision'], request_id='suspend')
        suspended = wait(lambda: request(op='job', id='portable'), lambda j: j['phase'] == 'suspended')
        assert all(json.loads(p.read_text())['state'] == 'deleted' for p in source.glob('backend/instances/*/observed.json'))
        stop('source')
        source.rename(a.home / 'unavailable-source')
        client.rename(a.home / 'unavailable-client')
        shutil.rmtree(inputs)
        import_client()
        stop('control')
        start('control')
        wait(lambda: request(op='storage'), lambda s: any(r['report'] is not None for r in s['storage_reports']))
        request(op='drain', worker='destination', drain=False, request_id='enable-destination')
        command('--home', client, 'resume', 'portable', '--worker', 'destination', '--request-id', 'resume-on-destination', '--revision', suspended['revision'])
        request(op='retry', request_id='resume-on-destination')
        final = wait(lambda: request(op='job', id='portable'), lambda j: j['phase'] in ('succeeded', 'failed', 'cancelled'), seconds=300)
        assert final['phase'] == 'succeeded', final
        assert final['assignment']['worker'] == 'destination' and len(final['attempts']) == 2
        request(op='fetch', id='portable', destination=str(a.home / 'outputs'))
        assert (a.home / 'outputs/result.txt').read_text() == '8'
        assert request(op='logs', id='portable')['Logs']['data'].count('resumed-once') == 1
        assert final['attempts'][0] == suspended['attempts'][0]
        result.update(cross_worker_resume=True, original_worker_and_client_unavailable=True, durable_executable_and_inputs=True,
                      cooperative_cleanup=True, controller_storage_restart=True, idempotent_resume=True, stable_job_identity=True)
        save('after-resume.json', final)
        input_artifact = next(r['artifact'] for r in request(op='artifacts') if r['artifact']['manifest']['id'] == bundle)
        refused = api(binary, client, dict(op='delete_artifact', id=bundle, expected_revision=input_artifact['revision'], request_id='cannot-delete-dependent-inputs'))
        assert refused['ok'] and 'Rejected' in refused['result']['reply'], refused
        (a.home / 'unavailable-source').rename(source)
        start('source')
        wait(lambda: request(op='artifacts'), lambda rows: any(r['artifact']['manifest']['kind'] == 'FinalOutputs' and r['artifact']['copies']['storage/backup']['phase'] == 'Ready' for r in rows))
        for kind in ('checkpoint-and-outputs', 'inputs'):
            for row in request(op='artifacts'):
                art = row['artifact']
                if (art['manifest']['kind'] == 'Inputs') == (kind == 'inputs'):
                    request(op='delete_artifact', id=art['manifest']['id'], expected_revision=art['revision'], request_id='delete-' + art['manifest']['id'])
        wait(lambda: request(op='artifacts'), lambda rows: all(r['artifact']['deleted'] and all(c['phase'] == 'Deleted' for c in r['artifact']['copies'].values()) for r in rows))
        result.update(dependency_retention_and_cleanup=True, success=True)
    finally:
        # Restore only this fixture's source directory so its own service can honor cleanup intent.
        if (a.home / 'unavailable-source').exists():
            (a.home / 'unavailable-source').rename(source)
        try:
            if 'control' not in processes:
                start('control')
            for name in ('source', 'destination'):
                if name not in processes:
                    start(name)
            for j in request(op='queue'):
                if j['phase'] not in ('succeeded', 'failed', 'cancelled'):
                    request(op='cancel', id=j['id'], expected_revision=j['revision'], request_id='cleanup-' + j['id'])
                    wait(lambda: request(op='job', id=j['id']), lambda j: j['phase'] in ('succeeded', 'failed', 'cancelled'))
        finally:
            for name in list(processes):
                stop(name)
            for log in logs:
                log.close()
            states = [json.loads(p.read_text())['state'] for home in (source, destination) for p in home.glob('backend/instances/*/observed.json')]
            result.update(fixture_vms_created=len(states), all_fixture_vms_deleted=all(s == 'deleted' for s in states), legacy_records_unchanged=preservation() == before)
            if normal:
                result['normal_records_unchanged'] = normal_before == dict(jobs=normal(op='queue'), instances=normal(op='instances'))
            save('qualification.json', result)
            assert result['all_fixture_vms_deleted'] and result['legacy_records_unchanged'] and result.get('normal_records_unchanged', True)
            for cache in caches:
                if cache.exists():
                    shutil.rmtree(cache)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
