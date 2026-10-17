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
    p.add_argument('--storage-lifecycle', action='store_true', help='replace and retire storage, then qualify explicit lost-agent repair')
    p.add_argument('--history', action='store_true', help='qualify archived logs/JSONL metrics across both attempts with source offline')
    p.add_argument('--instances', type=Path)
    p.add_argument('--image-cache', type=Path)
    a = p.parse_args()
    if a.storage_lifecycle:
        a.history = True
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
    backup_id = 'backup'
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
    command('service', 'configure', '--config', a.home / 'control.json', '--controller-home', a.home / 'seed/node-1',
            *([] if a.storage_lifecycle else ['--storage-home', storage]), '--bootstrap')
    if a.storage_lifecycle:
        command('service', 'configure', '--config', a.home / 'backup.json', '--storage-home', storage)
        for store_id in ('replacement', 'reserve', 'recovery'):
            command('cluster', 'prepare-storage', '--home', a.home / store_id, '--config', client / 'config.json',
                    '--id', store_id, '--failure-domain', 'same-qualification-machine', '--capacity-bytes', 64 << 20)
            command('service', 'configure', '--config', a.home / f'{store_id}.json', '--storage-home', a.home / store_id)
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
    if a.history:
        text = trainer.read_text().replace("assert pathlib.Path('dataset').read_text() == 'durable original data'", """assert pathlib.Path('dataset').read_text() == 'durable original data'
with open('metrics.jsonl', 'w') as metric:
 for step in range(2000): metric.write(json.dumps({'step':step,'loss':1/(step+1),'phase':sys.argv[1]})+'\\n')
if sys.argv[1]=='start': sys.stdout.write('x'*(9<<20)+'\\n')
print('diagnostic-'+sys.argv[1], file=sys.stderr, flush=True)""")
        trainer.write_text(text)
    trainer.chmod(0o700)
    (inputs / 'dataset').write_text('durable original data')
    job = dict(name='Portable qualification', argv=['bin/trainer', 'start'], inputs={'bin/trainer': str(trainer), 'dataset': str(inputs / 'dataset')},
               publish=['result.txt'], keep_awake=False, settle_seconds=0, max_seconds=600, threads=1 if a.fake else 2,
               vm=dict(image='macos', runtime=runtime, cpus=1 if a.fake else 2, memory_mb=4096 if a.fake else 6144, disk_gb=50, network='offline', gpu='exclusive'),
               checkpoint=dict(manifests='cp/*/fleet.json', resume_argv=['bin/trainer', 'resume', '{checkpoint}/state']))
    if a.history:
        job['metric_files'] = ['metrics.jsonl']
    try:
        start('control')
        if a.storage_lifecycle:
            start('backup')
        for home in (source, destination):
            command('--home', client, 'enroll', home / 'enroll.json', '--request-id', 'enroll-' + home.name)
        command('--home', client, 'storage', 'enroll', storage / 'registration.json', '--request-id', 'enroll-backup')
        command('--home', client, 'storage', 'policy', '--revision', 0, '--input', 'backup', '--checkpoint', 'backup', '--output', 'backup', *(['--history', 'backup'] if a.history else []), '--request-id', 'policy')
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
        wait(lambda: request(op='artifacts'), lambda rows: any(r['artifact']['manifest']['kind'] == {'Checkpoint': checkpoint['id']} and r['artifact']['copies'].get(f'storage/{backup_id}', {})['phase'] == 'Ready' for r in rows))
        save('before-suspend.json', request(op='job', id='portable'))
        request(op='suspend', id='portable', checkpoint=checkpoint['id'], expected_revision=initial['revision'], request_id='suspend')
        suspended = wait(lambda: request(op='job', id='portable'), lambda j: j['phase'] == 'suspended')
        assert all(json.loads(p.read_text())['state'] == 'deleted' for p in source.glob('backend/instances/*/observed.json'))
        if a.history:
            def backed(number):
                h = request(op='history', id='portable')
                streams = h['attempts'][number-1]['streams']
                return all(s['progress'] and s['progress']['closed'] and s['progress']['published'] == s['progress']['observed'] and s['backed_up_bytes'] == s['retained_bytes'] and s['backed_up_bytes'] > 0 for s in streams)
            wait(lambda: backed(1), bool)
        stop('source')
        source.rename(a.home / 'unavailable-source')
        client.rename(a.home / 'unavailable-client')
        shutil.rmtree(inputs)
        import_client()
        stop('control')
        start('control')
        wait(lambda: request(op='storage'), lambda s: any(r['report'] is not None for r in s['storage_reports']))
        if a.storage_lifecycle:
            for store_id in ('replacement', 'reserve', 'recovery'):
                command('--home', client, 'storage', 'enroll', a.home / store_id / 'registration.json', '--request-id', 'enroll-' + store_id)
            # Pin the selected checkpoint; retiring a copy must preserve artifact-level pins.
            cp_artifact = next(r['artifact'] for r in request(op='artifacts') if r['artifact']['manifest']['kind'] == {'Checkpoint': checkpoint['id']})
            request(op='artifact_retention', id=cp_artifact['manifest']['id'], expected_revision=cp_artifact['revision'],
                    pinned=True, expires_at=None, request_id='pin-during-replacement')
            command('--home', client, 'storage', 'replace', 'backup', 'replacement', '--revision', 0, '--request-id', 'replace-backup')
            request(op='retry', request_id='replace-backup')
            plan = request(op='storage_plan', storage='backup')
            assert plan['awaiting_replacement'], plan
            rejected = api(binary, client, dict(op='retire_storage', storage='backup', expected_revision=1,
                                               expected_inventory=plan['inventory'], request_id='too-early-retirement'))
            assert rejected['ok'] and 'Rejected' in rejected['result']['reply'], rejected
            start('replacement')
            wait(lambda: request(op='storage_plan', storage='backup'), lambda p: not p['awaiting_replacement'])
            stop('replacement'); start('replacement')
            plan = wait(lambda: request(op='storage_plan', storage='backup'), lambda p: not p['awaiting_replacement'])
            command('--home', client, 'storage', 'retire', 'backup', '--revision', 1, '--inventory', plan['inventory'], '--request-id', 'retire-backup')
            wait(lambda: request(op='storage_plan', storage='backup'), lambda p: not p['awaiting_cleanup'])
            command('--home', client, 'storage', 'finish-retirement', 'backup', '--revision', 2, '--request-id', 'finish-backup')
            stop('backup'); start('backup')
            assert request(op='storage_plan', storage='backup')['lifecycle']['phase'] == 'retired'
            stop('backup')
            assert not any((storage / 'objects').iterdir())
            backup_id = 'replacement'
            cp_artifact = next(r['artifact'] for r in request(op='artifacts') if r['artifact']['manifest']['id'] == cp_artifact['manifest']['id'])
            assert cp_artifact['pinned'] and not cp_artifact['deleted']
            result.update(storage_replacement=True, retirement_verified_before_cleanup=True, retirement_restart_fenced=True,
                          replacement_preserves_pins=True, replacement_with_source_offline=True)
        if a.history:
            def read_stream(number, op, **extra):
                offset, text = 0, []
                while True:
                    page = request(op=op, id='portable', attempt=number, **{'from':offset}, max=1<<20, **extra)
                    if op == 'logs': page = page['Logs']
                    assert page.get('gap') is None, page
                    assert page.get('archived') is True or page['data'] == '', page
                    text.append(page['data'])
                    if page['done']: return ''.join(text)
                    assert page['next'] > offset, page
                    offset = page['next']
            assert read_stream(1, 'logs').endswith('initial-once\n')
            assert read_stream(1, 'logs', stderr=True) == 'diagnostic-start\n'
            metrics = [json.loads(line) for line in read_stream(1, 'metrics').splitlines()]
            records = [m for m in metrics if m['kind'] == 'jsonl']
            assert len(records) == 2000 and [m['record']['step'] for m in records] == list(range(2000))
            result.update(archived_logs_over_old_limit=True, declared_metrics_exactly_once=True, offline_worker_history_reads=True)
        request(op='drain', worker='destination', drain=False, request_id='enable-destination')
        command('--home', client, 'resume', 'portable', '--worker', 'destination', '--request-id', 'resume-on-destination', '--revision', suspended['revision'])
        request(op='retry', request_id='resume-on-destination')
        final = wait(lambda: request(op='job', id='portable'), lambda j: j['phase'] in ('succeeded', 'failed', 'cancelled'), seconds=300)
        assert final['phase'] == 'succeeded', final
        assert final['assignment']['worker'] == 'destination' and len(final['attempts']) == 2
        # Completion and artifact publication are asynchronous; wait for the selected attempt's verified output copy.
        wait(lambda: request(op='artifacts'), lambda rows: any(
            r['artifact']['manifest']['kind'] == 'FinalOutputs'
            and r['artifact']['manifest']['origin'].get('Attempt',{}).get('attempt') == final['assignment']['id']
            and r['artifact']['copies'].get(f'storage/{backup_id}',{}).get('phase') == 'Ready' for r in rows))
        request(op='fetch', id='portable', destination=str(a.home / 'outputs'))
        assert (a.home / 'outputs/result.txt').read_text() == '8'
        assert request(op='logs', id='portable')['Logs']['data'].count('resumed-once') == 1
        if a.history:
            wait(lambda: backed(2), bool)
            assert read_stream(2, 'logs') == 'resumed-once\n'
            assert read_stream(2, 'logs', stderr=True) == 'diagnostic-resume\n'
            assert len([m for m in map(json.loads, read_stream(2, 'metrics').splitlines()) if m['kind'] == 'jsonl']) == 2000
            h = request(op='history', id='portable')
            assert h['attempts'][0]['worker'] == 'source' and h['attempts'][1]['worker'] == 'destination'
            policy = h['retention']['policy']; policy['log_days'] = 7
            updated = request(op='history_retention', id='portable', expected_revision=h['retention']['revision'], policy=policy, request_id='history-retention')
            assert updated['reply'] == 'Recorded', updated
            result['cross_attempt_history_and_retention_cli'] = True
        assert final['attempts'][0] == suspended['attempts'][0]
        result.update(cross_worker_resume=True, original_worker_and_client_unavailable=True, durable_executable_and_inputs=True,
                      cooperative_cleanup=True, controller_storage_restart=True, idempotent_resume=True, stable_job_identity=True)
        save('after-resume.json', final)
        if a.storage_lifecycle:
            status = request(op='storage')
            request(op='storage_policy', expected_revision=status['revision'],
                    policy={k:['replacement','reserve'] for k in ('inputs','checkpoints','outputs','history')}, request_id='redundant-backups')
            start('reserve')
            wait(lambda: request(op='artifacts'), lambda rows: all(r['artifact']['copies'].get('storage/reserve',{}).get('phase') == 'Ready' for r in rows))
            stop('replacement')
            (a.home/'replacement').rename(a.home/'lost-replacement')
            plan = request(op='storage_plan', storage='replacement')
            assert not plan['unavailable_without_agent'], plan
            command('--home', client, 'storage', 'declare-lost', 'replacement', 'recovery', '--revision', 0,
                    '--inventory', plan['inventory'], '--request-id', 'declare-lost-replacement')
            request(op='retry', request_id='declare-lost-replacement')
            start('recovery')
            wait(lambda: request(op='storage_plan', storage='replacement'), lambda p: not p['awaiting_replacement'])
            # A returning old disk cannot regain its identity, even though its payloads remain.
            (a.home/'lost-replacement').rename(a.home/'replacement')
            start('replacement')
            assert request(op='storage_plan', storage='replacement')['lifecycle']['phase'] == 'lost'
            stop('replacement')
            assert any((a.home/'replacement/objects').iterdir())
            backup_id = 'recovery'
            stop('destination')
            assert read_stream(2, 'logs') == 'resumed-once\n'
            request(op='fetch', id='portable', destination=str(a.home/'recovered-outputs'))
            assert (a.home/'recovered-outputs/result.txt').read_text() == '8'
            start('destination')
            result.update(lost_agent_repaired_from_survivor=True, returning_lost_identity_fenced=True,
                          lost_copy_erasure_unconfirmed=True, restored_history_and_outputs_without_workers=True)
        input_artifact = next(r['artifact'] for r in request(op='artifacts') if r['artifact']['manifest']['id'] == bundle)
        refused = api(binary, client, dict(op='delete_artifact', id=bundle, expected_revision=input_artifact['revision'], request_id='cannot-delete-dependent-inputs'))
        assert refused['ok'] and 'Rejected' in refused['result']['reply'], refused
        (a.home / 'unavailable-source').rename(source)
        start('source')
        wait(lambda: request(op='artifacts'), lambda rows: any(r['artifact']['manifest']['kind'] == 'FinalOutputs' and r['artifact']['copies'].get(f'storage/{backup_id}', {})['phase'] == 'Ready' for r in rows))
        for kind in ('checkpoint-and-outputs', 'inputs'):
            for row in request(op='artifacts'):
                art = row['artifact']
                if (art['manifest']['kind'] == 'Inputs') == (kind == 'inputs'):
                    if art['pinned']:
                        request(op='artifact_retention', id=art['manifest']['id'], expected_revision=art['revision'], pinned=False, expires_at=None, request_id='unpin-' + art['manifest']['id'])
                        art = next(r['artifact'] for r in request(op='artifacts') if r['artifact']['manifest']['id'] == art['manifest']['id'])
                    request(op='delete_artifact', id=art['manifest']['id'], expected_revision=art['revision'], request_id='delete-' + art['manifest']['id'])
        wait(lambda: request(op='artifacts'), lambda rows: all(r['artifact']['deleted'] and all(c['phase'] in ('Deleted','Lost') for c in r['artifact']['copies'].values()) for r in rows))
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
