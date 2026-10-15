#!/usr/bin/env python3
"""Qualify three direct-LAN replicas without changing installed Fleet services or host networking.

The JSON inventory contains three hosts: name, address (IPv4), binary (absolute), and ssh
(an argv array, empty for the local client host). Binaries must already be installed in isolated
locations. The first host is local. All experiment state uses one NEW --home under /tmp.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import select
import shlex
import subprocess
import time

HELPER = Path(__file__).with_name('control_host_fixture.py').read_text()


class Host:
    def __init__(self, spec, root):
        self.spec, self.root = spec, root
        self.started = set()
        self.sessions = {}
        self.before = None

    def command(self, args, stdin=None, timeout=50, check=True):
        argv = self.spec['ssh'] + [shlex.join(list(map(str, args)))] if self.spec['ssh'] else list(map(str, args))
        p = subprocess.run(argv, input=stdin, capture_output=True, text=True, timeout=timeout)
        if check and p.returncode:
            operation = '<Python helper>' if '-c' in args and args[0] == 'python3' else args[1:]
            raise RuntimeError(f"{self.spec['name']}: {operation}: {p.stderr[-5000:]}")
        return p

    def helper(self, op, **kwargs):
        p = self.command(['python3', '-c', HELPER], json.dumps({'root': str(self.root), 'op': op, **kwargs}))
        return json.loads(p.stdout)

    def write(self, path, value, raw=False):
        data = value if raw else json.dumps(value, indent=2).encode()
        return self.helper('write', path=path, data=base64.b64encode(data).decode())

    def read(self, path, raw=False):
        data = self.helper('read', path=path)
        if data is None:
            return None
        data = base64.b64decode(data)
        return data if raw else json.loads(data)

    def cli(self, *args, check=True, timeout=50):
        p = self.command([self.spec['binary'], *args], check=check, timeout=timeout)
        if not check:
            return p
        return json.loads(p.stdout) if p.stdout.strip() else None

    def spawn(self, name, *args):
        command = [self.spec['binary'], *map(str, args)]
        if not self.spec['ssh']:
            self.helper('spawn', name=name, command=command)
            self.started.add(name)
            return
        # The remote helper stays in this SSH session until its recorded child exits.
        # Closing the session immediately can revoke LAN access from an orphaned macOS child.
        log = (self.root / f"ssh-{self.spec['name']}-{name}.log").open('ab')
        try:
            session = subprocess.Popen(self.spec['ssh'] + [shlex.join(['python3', '-c', HELPER])],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log)
        finally:
            log.close()
        self.sessions[name] = session
        self.started.add(name)
        request = {'root': str(self.root), 'op': 'spawn', 'name': name,
                   'command': command, 'hold_session': True}
        session.stdin.write(json.dumps(request).encode())
        session.stdin.close()
        ready, _, _ = select.select([session.stdout], [], [], 15)
        if not ready:
            raise RuntimeError(f"{self.spec['name']}: SSH launch did not become ready: {name}")
        line = session.stdout.readline()
        if not line:
            raise RuntimeError(f"{self.spec['name']}: SSH launch failed: {name}; see its SSH log")
        json.loads(line)

    def stop(self, name, kill=False):
        if name in self.started:
            self.helper('stop', name=name, kill=kill)
            self.started.remove(name)
        session = self.sessions.pop(name, None)
        if session is not None:
            try:
                session.wait(timeout=15)
            except subprocess.TimeoutExpired:
                session.terminate()
                session.wait(timeout=5)
            finally:
                session.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--home', type=Path, required=True)
    parser.add_argument('--port', type=int, default=28460)
    a = parser.parse_args()
    assert a.home.is_absolute()
    specs = json.loads(a.inventory.read_text())['hosts']
    assert len(specs) == 3 and len({h['address'] for h in specs}) == 3
    assert not specs[0]['ssh'], 'first host must be the local client/recovery host'
    hosts = [Host(h, a.home) for h in specs]
    local = hosts[0]
    began_run = time.monotonic()
    source = Path(__file__).resolve().parent.parent
    source_files = sorted(source.joinpath('src').rglob('*.rs')) + [source / p for p in [
        'Cargo.toml', 'Cargo.lock', 'contrib/qualify_control_multihost.py', 'contrib/control_host_fixture.py']]
    result = {'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'source_sha256': {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files},
              'phases': [], 'hosts': [], 'limits': ['application-level partitions; no host firewall changes',
              'process crash/restart; no physical host power-off', 'native canaries; no new Tart hypervisor qualification']}
    replicas, jobs = {}, []
    key, config = a.home / 'cluster/client/node.key', a.home / 'cluster/client/config.json'
    creds = ['--key', key, '--config', config]

    def phase(name, **details):
        result['phases'].append({'name': name, **details})
        print(json.dumps(result['phases'][-1]), flush=True)

    def read():
        return local.cli('read', *creds)

    def wait(predicate, seconds=60):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            last = read()
            if predicate(last):
                return last
            time.sleep(.2)
        raise RuntimeError(f'condition timed out; last applied: {last.get("last_applied")}')

    def request(name, mutation, check=True, replica=None):
        origin = json.loads(config.read_text())['cluster']
        path = a.home / f'requests/{name}.json'
        local.write(f'requests/{name}.json', {'origin': origin, 'id': name, 'mutation': mutation})
        args = ['request', *creds, '--request', path]
        if replica is not None:
            args += ['--replica', replica]
        return local.cli(*args, check=check)

    def inspect(replica):
        # Individual startup configs retain the peer even after it leaves current membership.
        h, directory, _ = replicas[replica]
        private = h.read(f'{directory}/config.json')
        path = f'inspect-{replica}.json'
        local.write(path, private)
        return local.cli('inspect', '--key', key, '--config', a.home / path, '--replica', replica)

    def start(replica, bootstrap=False):
        h, directory, _ = replicas[replica]
        args = ['serve', '--home', a.home / directory, '--config', a.home / directory / 'config.json',
                '--qualification-partition', a.home / f'partition-{replica}.json']
        if bootstrap:
            args += ['--bootstrap']
        h.spawn(f'node-{replica}', *args)

    def caught_up(ids, seconds=60):
        barrier = read()['last_applied']['index']
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            statuses = [inspect(i) for i in ids]
            if all(s['last_applied'] and s['last_applied']['index'] >= barrier for s in statuses):
                return statuses
            time.sleep(.3)
        raise RuntimeError(f'replicas did not reach committed barrier {barrier}: {statuses}')

    def prepare(replica, h, public_config):
        directory = f'node-{replica}'
        h.write(f'base-{replica}.json', public_config)
        port = a.port + replica
        h.helper('port', address=h.spec['address'], port=port)
        descriptor = h.cli('prepare-replica', '--home', a.home / directory,
                           '--config', a.home / f'base-{replica}.json', '--id', replica,
                           '--bind', f"{h.spec['address']}:{port}")
        replicas[replica] = (h, directory, descriptor[str(replica)])
        h.write(f'partition-{replica}.json', [])
        start(replica)

    def membership(name, ids):
        before = read()
        voters = {str(i): replicas[i][2] for i in ids}
        local.write(f'{name}-voters.json', voters)
        reply = local.cli('membership', *creds, '--origin', before['cluster'], '--request-id', name,
                          '--expected-revision', before['configuration']['revision'],
                          '--voters', a.home / f'{name}-voters.json', '--wait-seconds', 90, timeout=110)
        assert reply['stage'] == 'Complete'
        state = read()
        assert state['configuration']['active'] is None
        return state

    def submit(h, name, duration):
        body = f'echo started-once; cat input.txt > result.txt; sleep {duration}'
        local.write(f'{name}-input.txt', f'{name}-verified\n'.encode(), raw=True)
        job = {'name': name, 'argv': ['/bin/sh', '-c', body], 'threads': 1, 'keep_awake': False,
               'max_seconds': duration + 30, 'shutdown_grace_seconds': 0, 'settle_seconds': 0,
               'inputs': {'input.txt': str(a.home / f'{name}-input.txt')}, 'publish': ['result.txt']}
        local.write(f'{name}-job.json', job)
        args = ['submit-job', *creds, '--data-home', a.home / 'client-data', '--job', a.home / f'{name}-job.json',
                '--worker', h.spec['name'], '--id', name, '--request-id', f'submit-{name}', '--memory-mb', 64]
        assert 'Accepted' in local.cli(*args)
        jobs.append((h, name))
        return args

    def running(name):
        return wait(lambda s: s['assignments'][name]['observed'] and
                    s['assignments'][name]['observed'][1]['phase'] == {'Job': 'Running'})

    def supervisor(h, name):
        return h.read(f'worker/backend/jobs/exec-{name}/supervisor.json')

    def fetch(name):
        local.cli('fetch', *creds, '--data-home', a.home / 'capture-data', '--assignment', name,
                  '--destination', a.home / f'outputs-{name}')
        assert (a.home / f'outputs-{name}/result.txt').read_text() == f'{name}-verified\n'

    try:
        for h in hosts:
            info = h.helper('create')
            h.before = info.pop('preservation')
            local.write(f"preservation-{h.spec['name']}-before.json", h.before)
            digest = h.command(['python3', '-c',
                'import hashlib,pathlib,sys;print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())',
                h.spec['binary']]).stdout.strip()
            result['hosts'].append({**info, 'name': h.spec['name'], 'address': h.spec['address'],
                                    'binary_sha256': digest})
            h.helper('port', address=h.spec['address'], port=a.port)
        assert len({h['hostname'] for h in result['hosts']}) == 3, 'three distinct physical hosts required'
        args = ['init', '--home', a.home / 'cluster', '--replicas', 3]
        for h in hosts:
            args += ['--address', f"{h.spec['address']}:{a.port}"]
        base = local.cli(*args)
        for i, h in enumerate(hosts, 1):
            for file in ['config.json', 'node.key']:
                h.write(f'node-{i}/{file}', (a.home / f'cluster/node-{i}/{file}').read_bytes(), raw=True)
            replicas[i] = (h, f'node-{i}', base['peers'][str(i)])
            h.write(f'partition-{i}.json', [])
            start(i, bootstrap=i == 1)
        statuses = caught_up([1, 2, 3])
        phase('three_physical_replicas_online', applied=[s['last_applied']['index'] for s in statuses])
        for h in hosts[:2]:
            h.write('worker-base.json', base)
            h.cli('prepare-worker', '--home', a.home / 'worker', '--config', a.home / 'worker-base.json',
                  '--id', h.spec['name'], '--cpu-slots', 1, '--memory-mb', 256,
                  '--bind', f"{h.spec['address']}:{a.port + 20}")
            local.write(f"enroll-{h.spec['name']}.json", h.read('worker/enroll.json'))
            local.cli('request', *creds, '--request', a.home / f"enroll-{h.spec['name']}.json")
            h.spawn('worker', 'worker', '--home', a.home / 'worker')
        submit(hosts[1], 'continuity', 720)
        running('continuity')
        original = supervisor(hosts[1], 'continuity')
        submit(local, 'short', 1)
        wait(lambda s: s['assignments']['short']['observed'] and s['assignments']['short']['observed'][1]['phase'] == {'Job': 'Succeeded'})
        fetch('short')
        phase('native_workers_and_verified_cross_host_transfers')

        active = [1, 2, 3]
        leader = next(i for i in active if inspect(i)['leader'] == i)
        began = time.monotonic()
        replicas[leader][0].stop(f'node-{leader}', kill=True)
        read()
        assert supervisor(hosts[1], 'continuity') == original
        phase('leader_sigkill_failover', seconds=round(time.monotonic() - began, 3), killed=leader)
        start(leader)
        caught_up(active)

        leader = next(i for i in active if inspect(i)['leader'] == i)
        for i in active:
            blocked = [replicas[j][2]['identity'] for j in active if (i == leader) != (j == leader)]
            replicas[i][0].write(f'partition-{i}.json', blocked)
        time.sleep(1.2)
        assert inspect(leader)['blocked_peers']
        isolated = local.cli('read', *creds, '--replica', leader, check=False)
        assert isolated.returncode != 0, 'minority served a strong read'
        mutation = {'Enqueue': {'id': 'partition', 'workload': {'Job': {'spec': {'name': 'metadata canary', 'argv': ['/bin/true'], 'threads': 1}, 'memory_mb': 128, 'gpu': False}}, 'worker': None, 'requires': ['qualification:never-schedule']}}
        failed = request('partition-write', mutation, check=False, replica=leader)
        assert failed.returncode != 0, 'minority acknowledged a mutation'
        assert 'partition' not in read()['jobs']
        for i in active:
            replicas[i][0].write(f'partition-{i}.json', [])
        time.sleep(.4)
        first = request('partition-write', mutation)
        assert request('partition-write', mutation) == first
        phase('partition_refuses_minority_and_retry_is_idempotent', isolated=leader)

        hosts[1].stop('worker', kill=True)
        hosts[1].spawn('worker', 'worker', '--home', a.home / 'worker')
        running('continuity')
        assert supervisor(hosts[1], 'continuity') == original
        leader = next(i for i in active if inspect(i)['leader'] == i)
        stopped = [i for i in active if i != leader]
        for i in stopped:
            replicas[i][0].stop(f'node-{i}', kill=True)
        assert local.cli('read', *creds, check=False).returncode != 0
        assert supervisor(hosts[1], 'continuity') == original
        for i in stopped:
            start(i)
        read()
        phase('worker_restart_and_quorum_outage_preserve_execution')

        leader = next(i for i in active if inspect(i)['leader'] == i)
        prepare(4, replicas[leader][0], base)
        target = [i for i in active if i != leader] + [4]
        membership('rotate-leader', target)
        replicas[leader][0].stop(f'node-{leader}')
        active = target
        survivor = next(i for i in active if replicas[i][0] is local)
        state = membership('shrink', [survivor])
        for i in active:
            if i != survivor:
                replicas[i][0].stop(f'node-{i}')
        public = {**base, 'peers': {str(survivor): replicas[survivor][2]}}
        prepare(5, hosts[1], public)
        prepare(6, hosts[2], public)
        state = membership('regrow', [survivor, 5, 6])
        active = [survivor, 5, 6]
        phase('leader_rotation_three_to_one_and_one_to_three', voters=active)

        # Keep a deliberately stale, stopped survivor while the other two commit real work.
        barrier = read()['last_applied']['index']
        end = time.monotonic() + 30
        while inspect(survivor)['last_applied']['index'] < barrier:
            assert time.monotonic() < end
            time.sleep(.2)
        local.stop(f'node-{survivor}')
        forgotten_request = submit(local, 'forgotten', 360)
        running('forgotten')
        forgotten_supervisor = supervisor(local, 'forgotten')
        for h in hosts[:2]:
            h.stop('worker', kill=True)
        for i in active:
            replicas[i][0].stop(f'node-{i}', kill=True)
        source_dir = replicas[survivor][1]
        manifest = local.cli('recover', '--source-home', a.home / source_dir,
                             '--config', a.home / source_dir / 'config.json', '--key', key,
                             '--home', a.home / 'recovered', '--bind', f"{local.spec['address']}:{a.port + 15}",
                             '--accept-data-loss', '--old-controllers-fenced')
        replicas[1] = (local, 'recovered', manifest['plan']['config']['peers']['1'])
        local.write('partition-1.json', [])
        start(1, bootstrap=True)
        local.cli('accept-recovery', '--config', config, '--manifest', a.home / 'recovered/recovery.json')
        assert 'forgotten' not in read()['assignments']
        assert 'RecoveryUnknown' in local.cli(*forgotten_request)
        for h in hosts[:2]:
            h.write('recovery.json', manifest)
            h.cli('rebind-worker', '--home', a.home / 'worker', '--manifest', a.home / 'recovery.json')
            h.spawn('worker', 'worker', '--home', a.home / 'worker')
        state = wait(lambda s: len(s['recovery']['inventories']) == 2)
        assert not state['recovery']['resumed'] and 'forgotten' in state['assignments']
        assert supervisor(hosts[1], 'continuity') == original
        assert supervisor(local, 'forgotten') == forgotten_supervisor
        assert 'RecoveryUnknown' in local.cli(*forgotten_request)
        assert local.cli('resume-recovery', *creds, '--origin', state['cluster'], '--request-id', 'resume') == 'Recorded'
        # A former controller key cannot read or command the new authority.
        denied = local.cli('read', '--key', a.home / source_dir / 'node.key', '--config', config, check=False)
        assert denied.returncode != 0
        phase('lost_quorum_recovery_rebinds_workers_without_duplicate_execution')
        new_config = json.loads(config.read_text())
        prepare(7, hosts[1], new_config)
        prepare(8, hosts[2], new_config)
        membership('regrow-recovered', [1, 7, 8])
        phase('recovered_cluster_returns_to_three_physical_voters')
        for h, name in [(hosts[1], 'continuity'), (local, 'forgotten')]:
            revision = read()['assignments'][name]['revision']
            request(f'cancel-{name}', {'Desire': {'id': name, 'expected_revision': revision, 'desired': {'Job': 'Cancelled'}}})
            wait(lambda s: s['assignments'][name]['observed'][1]['phase'] == {'Job': 'Cancelled'})
            assert h.read(f'worker/backend/jobs/exec-{name}/stdout.log', raw=True) == b'started-once\n'
            fetch(name)
        phase('cancellation_and_verified_outputs_after_recovery')
        # No surviving connection may hide an inability to form quorum after a cold restart.
        for i in [1, 7, 8]:
            replicas[i][0].stop(f'node-{i}', kill=True)
        for i in [7, 8]:
            start(i)
        state = read()
        assert all(state['assignments'][name]['observed'][1]['phase'] == {'Job': 'Cancelled'}
                   for name in ['continuity', 'forgotten'])
        assert state['recovery']['manifest'] == manifest and state['recovery']['resumed']
        assert len(state['recovery']['inventories']) == 2
        phase('cold_restart_remote_majority_without_original_host')
        result['success'] = True
    except Exception as e:
        result['success'] = False
        result['error'] = f'{type(e).__name__}: {e}'
        raise
    finally:
        errors = []
        for h, name in jobs:
            try:
                directory = f'worker/backend/jobs/exec-{name}'
                if h.read(f'{directory}/job.json') and not h.read(f'{directory}/exit.json'):
                    h.write(f'{directory}/cancel', b'qualification cleanup\n', raw=True)
                    end = time.monotonic() + 15
                    while not h.read(f'{directory}/exit.json') and time.monotonic() < end:
                        time.sleep(.2)
                    assert h.read(f'{directory}/exit.json'), f'canary did not exit: {name}'
            except Exception as e:
                errors.append(str(e))
        for h in hosts:
            for name in list(h.started):
                try:
                    h.stop(name)
                except Exception as e:
                    errors.append(str(e))
            if h.before is not None:
                try:
                    after = h.helper('preservation')
                    local.write(f"preservation-{h.spec['name']}-after.json", after)
                    assert after == h.before, f"installed Fleet state changed on {h.spec['name']}"
                except Exception as e:
                    errors.append(str(e))
        result['cleanup_errors'] = errors
        result['installed_state_unchanged'] = not errors
        result['fixture_processes_stopped'] = all(not h.started and not h.sessions for h in hosts)
        result['duration_seconds'] = round(time.monotonic() - began_run, 3)
        if local.before is not None:
            local.write('qualification.json', result)
        if errors:
            raise RuntimeError('; '.join(errors))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
