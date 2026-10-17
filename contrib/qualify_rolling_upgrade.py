#!/usr/bin/env python3
"""Two real build revisions, three local replicas, and an incompatible future fixture.

Only fresh /tmp/fleet-control-* homes and owned processes are touched. No launchd,
sudo, installed service, firewall, training job, or physical-host-loss claim.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from cli_fixture import api, api_result, wait_for
from control_host_fixture import preservation, validate_service


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'candidate', 'unsupported', 'home'):
        p.add_argument('--' + name, type=Path, required=True)
    a = p.parse_args()
    root = a.home.resolve()
    assert root.is_relative_to(Path('/tmp').resolve()) and root.name.startswith('fleet-control-')
    assert not root.exists(), 'use a fresh fixture home'
    for binary in (a.baseline, a.candidate, a.unsupported):
        assert binary.is_absolute() and binary.is_file()
    root.mkdir(mode=0o700)
    seed, client, worker, storage = [root / x for x in ('seed', 'client', 'worker', 'storage')]
    before = preservation()
    processes, logs, phases = {}, [], []
    binaries = {i: a.baseline for i in (1, 2, 3)}

    def command(binary, *args, good=True):
        out = subprocess.run([str(binary), *map(str, args)], capture_output=True, text=True, timeout=60)
        if good:
            assert out.returncode == 0, (args, out.stderr, out.stdout)
            return json.loads(out.stdout) if out.stdout.strip() else None
        return out

    def request(**kw):
        return api_result(a.candidate, client, **kw)

    def rejected(**kw):
        out = api(a.candidate, client, kw)
        assert out['ok'] and 'Rejected' in out['result']['reply'], out

    def state():
        return command(a.candidate, 'cluster', 'read', '--key', client / 'node.key', '--config', client / 'config.json')

    def inspect(node):
        return command(a.candidate, 'cluster', 'inspect', '--key', client / 'node.key', '--config', client / 'config.json', '--replica', node)

    def start(node):
        config = root / f'node-{node}.json'
        args = ['service', 'run', '--config', str(config)]
        validate_service(root, args)
        log = open(root / f'node-{node}.log', 'a')
        logs.append(log)
        proc = subprocess.Popen([str(binaries[node]), *args], stdout=log, stderr=log)
        processes[node] = proc

    def stop(node, kill=False):
        proc = processes.pop(node, None)
        if proc and proc.poll() is None:
            proc.kill() if kill else proc.terminate()
            proc.wait(timeout=30)

    def caught_up(nodes=(1, 2, 3)):
        barrier = state()['last_applied']['index']
        wait_for(lambda: [inspect(i) for i in nodes], lambda xs: all(
            x['last_applied'] and x['last_applied']['index'] >= barrier for x in xs), timeout=60)

    def unchanged_rejection(node, binary):
        path = seed / f'node-{node}/control.redb'
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        out = command(binary, 'service', 'run', '--config', root / f'node-{node}.json', good=False)
        assert out.returncode != 0 and 'incompatible' in out.stderr, out.stderr
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, 'rejection changed original database'
        out = command(binary, 'cluster', 'upgrade', '--controller-home', seed / f'node-{node}', good=False)
        assert out.returncode != 0 and 'incompatible' in out.stderr, out.stderr
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, 'offline upgrade rejection changed database'

    def phase(name):
        phases.append(name)
        print(json.dumps({'phase': name}), flush=True)

    def live():
        assert request(op='job', id='continuity')['phase'] == 'running'
        assert supervisor.read_bytes() == original_supervisor, 'supervisor or child execution changed'
        assert request(op='retry', request_id='submit-continuity')['reply'] == submitted['reply']

    try:
        command(a.baseline, 'cluster', 'init', '--home', seed, '--replicas', 3)
        command(a.baseline, '--home', client, 'client', 'import', '--config', seed / 'client/config.json', '--key', seed / 'client/node.key')
        command(a.baseline, 'cluster', 'prepare-worker', '--home', worker, '--config', client / 'config.json', '--id', 'canary', '--cpu-slots', 1, '--memory-mb', 256)
        command(a.baseline, 'cluster', 'prepare-storage', '--home', storage, '--config', client / 'config.json', '--id', 'backup', '--failure-domain', 'one-qualification-host', '--capacity-bytes', 32 << 20)
        for node in (1, 2, 3):
            args = ['service', 'configure', '--config', root / f'node-{node}.json', '--controller-home', seed / f'node-{node}']
            if node == 1:
                args += ['--worker-home', worker, '--storage-home', storage, '--bootstrap']
            command(a.baseline, *args)
            start(node)
        caught_up()
        assert all(inspect(i)['software'] == {'min': 1, 'max': 1} for i in (1, 2, 3))
        command(a.baseline, '--home', client, 'enroll', worker / 'enroll.json', '--request-id', 'enroll')
        command(a.baseline, '--home', client, 'storage', 'enroll', storage / 'registration.json', '--request-id', 'enroll-store')
        request(op='storage_policy', request_id='policy', expected_revision=0,
                policy={'checkpoints': [], 'outputs': ['backup'], 'inputs': [], 'history': ['backup']})
        gate = root / 'finish-canary'
        submitted = request(op='submit', request_id='submit-continuity', id='continuity', worker='canary', memory_mb=128,
                            job={'name': 'isolated rolling upgrade canary', 'argv': ['/bin/sh', '-c',
                                 'echo started-once; while [ ! -f "$1" ]; do sleep .1; done; echo finished-once; echo preserved > result.txt',
                                 'canary', str(gate)], 'threads': 1, 'keep_awake': False, 'max_seconds': 900,
                                 'shutdown_grace_seconds': 0, 'settle_seconds': 0, 'publish': ['result.txt']})
        wait_for(lambda: request(op='job', id='continuity'), lambda j: j['phase'] == 'running')
        supervisor = worker / 'backend/jobs/exec-continuity-a1/supervisor.json'
        wait_for(lambda: supervisor.exists(), bool)
        original_supervisor = supervisor.read_bytes()
        phase('baseline_three_replicas_with_combined_worker_and_storage')

        # An unsupported executable cannot mutate the cluster through the public API.
        out = api(a.unsupported, client, {'op': 'prepare_version', 'request_id': 'future-write', 'expected_revision': 0, 'target': 2})
        assert not out['ok'] and 'compatib' in str(out), out
        assert not any(k.endswith('future-write"]') for k in state()['receipts'])
        rejected(op='prepare_version', request_id='skip-generation', expected_revision=0, target=3)
        assert state()['version']['active'] == 1
        phase('unsupported_network_client_and_generation_jump_rejected')
        # A future binary may bootstrap its own fresh cluster at its floor; it must never
        # reinterpret the existing cluster's generation or join that older quorum.
        future = root / 'future-seed'
        command(a.unsupported, 'cluster', 'init', '--home', future, '--replicas', 1)
        command(a.unsupported, 'service', 'configure', '--config', root / 'node-4.json',
                '--controller-home', future / 'node-1', '--bootstrap')
        binaries[4] = a.unsupported
        start(4)
        fresh = command(a.unsupported, 'cluster', 'read', '--key', future / 'client/node.key', '--config', future / 'client/config.json')
        assert fresh['version']['active'] == 3
        assert state()['version']['active'] == 1
        stop(4)
        phase('fresh_future_cluster_uses_its_floor_without_upgrading_existing_state')

        # The first upgraded follower starts from an unclean database and a compacted log.
        leader = inspect(1)['leader']
        follower = next(i for i in (2, 3) if i != leader)
        stop(follower, kill=True)
        unchanged_rejection(follower, a.unsupported)
        for n in range(110):
            request(op='drain', request_id=f'snapshot-{n}', worker='canary', drain=bool(n % 2))
        request(op='drain', request_id='undrain', worker='canary', drain=False)
        wait_for(lambda: inspect(leader), lambda s: s['purged'] is not None)
        binaries[follower] = a.candidate
        start(follower)
        caught_up()
        assert inspect(follower)['software'] == {'min': 1, 'max': 2}
        assert inspect(follower)['snapshot'] is not None
        live()
        phase('dirty_database_rejection_and_mixed_version_snapshot_catchup')

        # Preparation cannot activate with old members. A persisted promise blocks rollback.
        request(op='prepare_version', request_id='prepare-first', expected_revision=0, target=2)
        wait_for(lambda: state()['version'], lambda v: follower in v['pending']['ready'])
        rejected(op='activate_version', request_id='early-activate', expected_revision=1)
        stop(follower, kill=True)
        unchanged_rejection(follower, a.baseline)
        request(op='abort_version', request_id='abort-first', expected_revision=1)
        # The offline follower must first learn the committed abort with the new executable.
        unchanged_rejection(follower, a.baseline)
        start(follower)
        caught_up()
        stop(follower)
        binaries[follower] = a.baseline
        start(follower)
        caught_up()
        live()
        phase('preparation_blocks_unsafe_rollback_and_abort_enables_safe_rollback')

        # Roll all services, including the process hosting live work, then exercise leader loss.
        leader = inspect(1)['leader']
        for node in [i for i in (1, 2, 3) if i != leader] + [leader]:
            stop(node)
            binaries[node] = a.candidate
            start(node)
            caught_up()
            live()
        revision = state()['version']['revision']
        request(op='prepare_version', request_id='prepare-final', expected_revision=revision, target=2)
        wait_for(lambda: state()['version'], lambda v: v['pending']['ready'] == [1, 2, 3])
        leader = inspect(1)['leader']
        stop(leader, kill=True)
        request(op='activate_version', request_id='activate-final', expected_revision=revision + 1)
        # A prepared old binary cannot reopen the stopped leader even before it sees activation.
        unchanged_rejection(leader, a.baseline)
        start(leader)
        caught_up()
        assert state()['version']['active'] == 2
        assert api_result(a.baseline, client, op='job', id='continuity')['phase'] == 'running'
        assert request(op='retry', request_id='activate-final')['reply'] == 'Recorded'
        live()
        phase('rolling_combined_services_leader_loss_and_idempotent_activation')

        for node in (1, 2, 3):
            stop(node, kill=True)
        for node in (1, 2, 3):
            unchanged_rejection(node, a.baseline)
            unchanged_rejection(node, a.unsupported)
        # No fresh quorum permission is needed to start any replica.
        for node in (1, 2, 3):
            start(node)
        caught_up()
        live()
        phase('cold_quorum_restart_and_post_activation_downgrade_refusal')

        gate.touch()
        wait_for(lambda: request(op='job', id='continuity'), lambda j: j['phase'] == 'succeeded')
        artifacts = wait_for(lambda: request(op='artifacts'), lambda xs: any(
            x['artifact']['manifest']['kind'] == 'FinalOutputs' and x['artifact']['copies'].get('storage/backup', {}).get('phase') == 'Ready'
            for x in xs), timeout=90)
        output = next(x['artifact'] for x in artifacts if x['artifact']['manifest']['kind'] == 'FinalOutputs')
        request(op='fetch_artifact', id=output['manifest']['id'], destination=str(root / 'restored'))
        assert (root / 'restored/result.txt').read_text() == 'preserved\n'
        expected_logs = 'started-once\nfinished-once\n'
        def stdout_history():
            return next(s for s in request(op='history', id='continuity')['attempts'][0]['streams'] if s['stream'] == 'stdout')
        wait_for(stdout_history, lambda h: h['progress']['closed'] and h['backed_up_bytes'] == len(expected_logs))
        offset, text = 0, ''
        for _ in range(len(expected_logs) + 1):
            page = request(op='logs', id='continuity', **{'from': offset})['Logs']
            assert page.get('gap') is None, page
            text += page['data']
            assert expected_logs.startswith(text), text
            if page['data']:
                assert page['archived'], page
            if page['done']:
                break
            assert page['next'] > offset, page
            offset = page['next']
        assert text == expected_logs, text
        assert len(request(op='attempts', id='continuity')) == 1
        phase('verified_output_backup_and_exactly_once_native_execution')
        assert preservation() == before, 'installed state changed'
        result = {'success': True, 'phases': phases, 'installed_state_unchanged': True,
                  'physical_failure_domain': 'one host; process faults only',
                  'binary_sha256': {name: hashlib.sha256(getattr(a, name).read_bytes()).hexdigest()
                                    for name in ('baseline', 'candidate', 'unsupported')}}
        (root / 'qualification.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)
    finally:
        # Release the owned native canary even if an assertion failed; its supervisor is detached.
        (root / 'finish-canary').touch()
        for node in list(processes):
            stop(node)
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
