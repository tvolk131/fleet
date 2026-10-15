#!/usr/bin/env python3
"""Normal CLI/Python queue and service roles on three replicas, in fresh /tmp homes.

Uses the same guarded fixture transport as the lower-level failure qualification. No
installed services, legacy state, host firewall or training configuration are modified.
"""
import argparse
import hashlib
import getpass
import json
import os
import plistlib
from pathlib import Path
import time

from control_host_fixture import process_identity, validate_service
from fleet import Fleet
from qualify_control_multihost import Host


class LaunchdHost(Host):
    """Real user launchd services; no administrator setup or permanent installation.

    System boot installation is qualified separately. Each test label is unique,
    and cleanup only unloads labels created by this fixture.
    """
    def __init__(self, spec, root):
        super().__init__(spec, root)
        self.domain = f"gui/{os.getuid()}"
        self.labels = {}
        self.generation = 0

    def spawn(self, name, *args):
        assert not self.spec['ssh'] and list(args[:3]) == ['service', 'run', '--config']
        config = Path(args[3])
        validate_service(self.root, args)
        self.generation += 1
        label = f"local.fleet.qualification.{os.getpid()}.{self.spec['name']}.{self.generation}"
        definition = self.root / f'{name}.plist'
        # The normal renderer supplies restart/lifecycle behavior. Adapt only the
        # system-service user and log paths to this isolated login-session fixture.
        rendered = self.root / f'{name}-render-{time.monotonic_ns()}.plist'
        self.cli('service', 'render', '--kind', 'launchd', '--name', label,
                            '--user', getpass.getuser(), '--user-home', Path.home(),
                            '--executable', self.spec['binary'], '--config', config, '--output', rendered)
        value = plistlib.loads(rendered.read_bytes())
        rendered.unlink()
        value.pop('UserName', None)
        value['StandardOutPath'] = value['StandardErrorPath'] = str(self.root / f'{name}.log')
        definition.write_bytes(plistlib.dumps(value))
        self.labels[name] = label
        self.started.add(name)
        self.command(['launchctl', 'bootstrap', self.domain, definition])

    def stop(self, name, kill=False):
        if name not in self.started:
            return
        target = f'{self.domain}/{self.labels[name]}'
        pid = self.pid(name)
        identity = process_identity(pid) if pid else None
        if kill:
            self.command(['launchctl', 'kill', 'SIGKILL', target], check=False)
        self.command(['launchctl', 'bootout', target])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            registered = self.command(['launchctl', 'print', target], check=False).returncode == 0
            alive = identity and process_identity(pid) == identity
            if not registered and not alive:
                self.started.remove(name)
                return
            time.sleep(.1)
        raise RuntimeError(f'launchd did not unload {name}')

    def pid(self, name):
        output = self.command(['launchctl', 'print', f'{self.domain}/{self.labels[name]}']).stdout
        for line in output.splitlines():
            if line.strip().startswith('pid = '):
                return int(line.split('=')[1])
        return None

    def crash_restart(self, name):
        old = self.pid(name)
        assert old is not None
        self.command(['launchctl', 'kill', 'SIGKILL', f'{self.domain}/{self.labels[name]}'])
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            new = self.pid(name)
            if new and new != old:
                return {'old_pid': old, 'new_pid': new}
            time.sleep(.25)
        raise RuntimeError(f'launchd failed to restart {name}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--inventory', type=Path)
    mode.add_argument('--local', action='store_true', help='Three separate local processes on loopback')
    parser.add_argument('--binary', type=Path, help='Explicit fleet executable for --local')
    parser.add_argument('--launchd', action='store_true', help='Use real user launchd services for --local (macOS)')
    parser.add_argument('--home', type=Path, required=True)
    parser.add_argument('--port', type=int, default=28900)
    a = parser.parse_args()
    assert a.home.is_absolute(), 'qualification home must be absolute'
    if a.local:
        assert a.binary and a.binary.is_file(), '--local needs --binary'
        specs = [dict(name=f'local-{i}', address='127.0.0.1', port=a.port + 100 * (i - 1),
                      binary=str(a.binary.resolve()), ssh=[]) for i in range(1, 4)]
    else:
        assert not a.launchd, '--launchd requires --local'
        specs = json.loads(a.inventory.read_text())['hosts']
        assert len(specs) == 3 and len({h['address'] for h in specs}) == 3 and not specs[0]['ssh']
    host_type = LaunchdHost if a.launchd else Host
    hosts = [host_type(h, a.home / h['name'] if a.local and i else a.home) for i, h in enumerate(specs)]
    local, m1, pi = hosts
    client = a.home / 'client'
    started = time.monotonic()
    result = {'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'phases': [], 'hosts': [],
              'topology': 'local-three' if a.local else 'physical-three',
              'service_manager': 'user-launchd' if a.launchd else 'fixture-processes',
              'limits': ['process crashes; no physical power-off',
                         'All replicas share one machine; no host/network independence' if a.local else 'SSH launch sessions held; installed-service launch qualified separately',
                         'tiny native canaries; no ML training or host networking changes']}
    replicas, jobs, service_configs = {}, [], {}
    source = Path(__file__).resolve().parents[1]
    paths = sorted((source / 'src').rglob('*.rs')) + [source / p for p in (
        'Cargo.toml', 'Cargo.lock', 'contrib/qualify_normal_multihost.py', 'contrib/control_host_fixture.py', 'contrib/fleet.py')]
    result['source_sha256'] = {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}

    def normal(*args, **kwargs):
        return local.cli('--home', client, *args, **kwargs)

    def admin(*args, **kwargs):
        return local.cli('cluster', *args, **kwargs)

    def state():
        return admin('read', '--key', client / 'node.key', '--config', client / 'config.json')

    def wait(call, predicate, seconds=75):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            last = call()
            if predicate(last):
                return last
            time.sleep(.3)
        raise RuntimeError(f'timed out: {str(last)[-3000:]}')

    def phase(name, **details):
        result['phases'].append(dict(name=name, **details))
        print(json.dumps(result['phases'][-1]), flush=True)

    def configure(h, name, controller=None, worker=False, bootstrap=False):
        index = len(service_configs) + 1
        config = h.root / f'{name}-config-{index}-{time.monotonic_ns()}.json'
        service_configs[h.spec['name'], name] = config
        args = ['service', 'configure', '--config', config]
        if controller:
            args += ['--controller-home', h.root / controller]
        if worker:
            args += ['--worker-home', h.root / 'worker']
        if bootstrap:
            args += ['--bootstrap']
        h.cli(*args)

    def start(i):
        h, directory, peer = replicas[i]
        h.spawn(f'node-{i}', 'service', 'run', '--config', service_configs[h.spec['name'], f'node-{i}'])

    def inspect(i):
        h, directory, peer = replicas[i]
        local.write(f'inspect-{i}.json', h.read(f'{directory}/config.json'))
        return admin('inspect', '--key', client / 'node.key', '--config', a.home / f'inspect-{i}.json', '--replica', i)

    def caught_up(ids):
        barrier = state()['last_applied']['index']
        return wait(lambda: [inspect(i) for i in ids], lambda items: all(s['last_applied'] and s['last_applied']['index'] >= barrier for s in items))

    def prepare(i, h, base):
        h.write(f'base-{i}.json', base)
        h.helper('port', address=h.spec['address'], port=a.port + i)
        peers = h.cli('cluster', 'prepare-replica', '--home', h.root / f'node-{i}', '--config', h.root / f'base-{i}.json',
                      '--id', i, '--bind', f"{h.spec['address']}:{a.port + i}")
        replicas[i] = (h, f'node-{i}', peers[str(i)])
        local.write(f'peer-{i}.json', peers)
        configure(h, f'node-{i}', f'node-{i}')
        start(i)

    def membership(name, add=(), remove=()):
        args = ['replicas', '--request-id', name]
        for i in add:
            args += ['--add', a.home / f'peer-{i}.json']
        for i in remove:
            args += ['--remove', i]
        answer = normal(*args)
        operation = answer['reply']['MembershipAccepted']['operation']
        wait(lambda: normal('operation', operation), lambda v: v['stage'] == 'Complete', seconds=100)
        assert normal('retry', name)['reply'] == answer['reply']
        return normal('replicas')

    def submit(h, name, duration, after=()):
        local.write(f'{name}-input.txt', f'{name}-verified\n'.encode(), raw=True)
        job = {'name': name, 'argv': ['/bin/sh', '-c', f'echo started-once; cat input.txt > result.txt; sleep {duration}'],
               'threads': 1, 'keep_awake': False, 'max_seconds': duration + 120, 'shutdown_grace_seconds': 0,
               'settle_seconds': 0, 'inputs': {'input.txt': str(a.home / f'{name}-input.txt')}, 'publish': ['result.txt']}
        local.write(f'{name}-job.json', job)
        args = ['submit', a.home / f'{name}-job.json', '--worker', h.spec['name'], '--id', name,
                '--request-id', f'submit-{name}', '--memory-mb', 128]
        for dep in after:
            args += ['--after', dep]
        assert normal(*args)['reply']['Accepted']['job'] == name
        jobs.append((h, name))

    def job_phase(name, phase):
        return wait(lambda: normal('job', name), lambda v: v['phase'] == phase)

    def supervisor(h, name):
        record = h.read(f'worker/backend/jobs/exec-{name}-a1/supervisor.json')
        assert record, 'missing supervisor record for active attempt'
        return record

    def fetch(name):
        normal('fetch', name, a.home / f'capture-{name}')
        assert (a.home / f'capture-{name}/result.txt').read_text() == f'{name}-verified\n'
        assert normal('logs', name)['Logs']['data'] == 'started-once\n'

    try:
        for h in hosts:
            info = h.helper('create')
            h.before = info.pop('preservation')
            local.write(f"preservation-{h.spec['name']}-before.json", h.before)
            digest = h.command(['python3', '-c', 'import hashlib,pathlib,sys;print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())',
                                h.spec['binary']]).stdout.strip()
            result['hosts'].append({**info, 'name': h.spec['name'], 'binary_sha256': digest})
            h.helper('port', address=h.spec['address'], port=h.spec.get('port', a.port))
        assert a.local or len({h['hostname'] for h in result['hosts']}) == 3
        args = ['init', '--home', a.home / 'seed', '--replicas', 3]
        for h in hosts:
            args += ['--address', f"{h.spec['address']}:{h.spec.get('port', a.port)}"]
        base = admin(*args)
        normal('client', 'import', '--config', a.home / 'seed/client/config.json', '--key', a.home / 'seed/client/node.key')
        for i, h in enumerate(hosts, 1):
            for file in ['config.json', 'node.key']:
                h.write(f'node-{i}/{file}', (a.home / f'seed/node-{i}/{file}').read_bytes(), raw=True)
            replicas[i] = (h, f'node-{i}', base['peers'][str(i)])
            if i < 3:
                h.write('base.json', base)
                h.cli('cluster', 'prepare-worker', '--home', h.root / 'worker', '--config', h.root / 'base.json',
                      '--id', h.spec['name'], '--cpu-slots', 1, '--memory-mb', 256, '--bind', f"{h.spec['address']}:{h.spec.get('port', a.port) + 20}")
            configure(h, f'node-{i}', f'node-{i}', worker=i < 3, bootstrap=i == 1)
            start(i)
        caught_up([1, 2, 3])
        for h in hosts[:2]:
            local.write(f"enroll-{h.spec['name']}.json", h.read('worker/enroll.json'))
            normal('enroll', a.home / f"enroll-{h.spec['name']}.json", '--request-id', f"enroll-{h.spec['name']}")
        assert normal('status')['quorum']
        phase('normal_combined_workers_and_controller_services')
        if a.launchd:
            for i, h in enumerate(hosts, 1):
                restarted = h.crash_restart(f'node-{i}')
                caught_up([1, 2, 3])
                phase('launchd_automatically_restarts_crashed_replica', replica=i, **restarted)
        submit(m1, 'continuity', 1500)
        job_phase('continuity', 'running')
        original = supervisor(m1, 'continuity')
        submit(local, 'short', 1)
        job_phase('short', 'succeeded')
        fetch('short')
        client_api = Fleet(client, local.spec['binary'])
        python_job = client_api.submit('Python qualification canary', ['/bin/sh', '-c', 'echo python-once'], worker=local.spec['name'],
                                      memory_mb=128, max_seconds=20, keep_awake=False, settle_seconds=0, job_id='python-canary', request_id='python-submit')
        jobs.append((local, python_job))
        job_phase(python_job, 'succeeded')
        assert client_api.logs(python_job)['data'] == 'python-once\n'
        assert normal('retry', 'submit-short')['reply']['Accepted']['job'] == 'short'
        assert len([j for j in normal('queue') if j['id'] == 'short']) == 1
        phase('normal_cli_python_queue_inputs_outputs_and_idempotent_retry')

        # Queued dependency reserves no additional capacity on the occupied one-slot M1.
        submit(m1, 'dependent', 1, after=['continuity'])
        assert normal('job', 'dependent')['assignment'] is None
        normal('cancel', 'dependent', '--request-id', 'cancel-dependent')
        assert normal('job', 'dependent')['phase'] == 'cancelled'
        leader = next(i for i in [1, 2, 3] if inspect(i)['leader'] == i)
        began = time.monotonic()
        replicas[leader][0].stop(f'node-{leader}', kill=True)
        assert normal('status')['quorum']
        assert supervisor(m1, 'continuity') == original
        start(leader)
        caught_up([1, 2, 3])
        # Explicitly crash the combined M1 process even if it was not the leader.
        m1.stop('node-2', kill=True)
        start(2)
        job_phase('continuity', 'running')
        assert supervisor(m1, 'continuity') == original
        phase('combined_service_crash_preserves_supervisor_and_queue', initial_leader=leader,
              crash_and_recovery_seconds=round(time.monotonic() - began, 3))

        # Continue membership tests with independent worker processes, a supported service role.
        for i, h in enumerate(hosts[:2], 1):
            h.stop(f'node-{i}')
            configure(h, f'node-{i}', f'node-{i}', bootstrap=i == 1)
            configure(h, 'worker-service', worker=True)
            start(i)
            h.spawn('worker-service', 'service', 'run', '--config', service_configs[h.spec['name'], 'worker-service'])
        caught_up([1, 2, 3])
        prepare(4, pi, base)
        membership('rotate-pi', add=[4], remove=[3])
        pi.stop('node-3')
        membership('shrink', remove=[2, 4])
        m1.stop('node-2')
        pi.stop('node-4')
        public = json.loads((client / 'config.json').read_text())
        public['peers'] = {'1': replicas[1][2]}
        prepare(5, m1, public)
        prepare(6, pi, public)
        membership('regrow', add=[5, 6])
        caught_up([1, 5, 6])
        phase('normal_replica_rotation_three_to_one_to_three')

        # The ordinary client persists intent before attempting delivery during an outage.
        m1.stop('node-5', kill=True)
        pi.stop('node-6', kill=True)
        local.write('outage.json', {'name': 'during outage', 'argv': ['true'], 'threads': 1, 'keep_awake': False})
        unknown = normal('submit', a.home / 'outage.json', '--id', 'outage', '--request-id', 'outage-submit', '--requires', 'never', check=False)
        assert unknown.returncode and 'outcome unknown' in unknown.stderr
        start(5)
        start(6)
        assert normal('retry', 'outage-submit')['reply']['Accepted']['job'] == 'outage'
        normal('cancel', 'outage')
        assert supervisor(m1, 'continuity') == original
        phase('unknown_submission_resolved_from_durable_client_journal')

        caught_up([1, 5, 6])
        local.stop('node-1')
        submit(local, 'forgotten', 600)
        job_phase('forgotten', 'running')
        forgotten = supervisor(local, 'forgotten')
        for h in hosts[:2]:
            h.stop('worker-service')
        for i in [5, 6]:
            replicas[i][0].stop(f'node-{i}', kill=True)
        manifest = admin('recover', '--source-home', a.home / 'node-1', '--config', a.home / 'node-1/config.json',
                         '--key', client / 'node.key', '--home', a.home / 'recovered', '--bind', f"{local.spec['address']}:{a.port + 15}",
                         '--accept-data-loss', '--old-controllers-fenced')
        replicas[1] = (local, 'recovered', manifest['plan']['config']['peers']['1'])
        configure(local, 'node-1', 'recovered', bootstrap=True)
        start(1)
        normal('accept-recovery', a.home / 'recovered/recovery.json')
        assert not any(j['id'] == 'forgotten' for j in normal('queue'))
        replay = normal('retry', 'submit-forgotten', check=False)
        assert replay.returncode and 'RecoveryUnknown' in replay.stdout
        for h in hosts[:2]:
            h.write('recovery.json', manifest)
            h.cli('cluster', 'rebind-worker', '--home', h.root / 'worker', '--manifest', h.root / 'recovery.json')
            h.spawn('worker-service', 'service', 'run', '--config', service_configs[h.spec['name'], 'worker-service'])
        wait(state, lambda v: len(v['recovery']['inventories']) == 2)
        assert normal('job', 'forgotten')['assignment']['execution'] == 'exec-forgotten-a1'
        assert supervisor(local, 'forgotten') == forgotten and supervisor(m1, 'continuity') == original
        normal('resume-recovery', '--request-id', 'resume')
        for name in ['continuity', 'forgotten']:
            normal('cancel', name, '--request-id', f'cancel-{name}')
            job_phase(name, 'cancelled')
            fetch(name)
        phase('normal_recovery_client_safeguard_inventory_visibility_and_cancellation')
        recovered_config = json.loads((client / 'config.json').read_text())
        prepare(7, m1, recovered_config)
        prepare(8, pi, recovered_config)
        membership('regrow-recovered', add=[7, 8])
        caught_up([1, 7, 8])
        for i in [1, 7, 8]:
            replicas[i][0].stop(f'node-{i}', kill=True)
        start(7)
        start(8)
        assert normal('status')['quorum']
        assert normal('job', 'forgotten')['phase'] == 'cancelled'
        phase('recovered_queue_survives_majority_cold_restart')
        result['success'] = True
    except Exception as error:
        result.update(success=False, error=f'{type(error).__name__}: {error}')
        raise
    finally:
        errors = []
        for h, name in jobs:
            try:
                directory = f'worker/backend/jobs/exec-{name}-a1'
                if h.read(f'{directory}/job.json') and not h.read(f'{directory}/exit.json'):
                    h.write(f'{directory}/cancel', b'qualification cleanup\n', raw=True)
                    wait(lambda: h.read(f'{directory}/exit.json'), bool, seconds=20)
            except Exception as error:
                errors.append(str(error))
        for h in hosts:
            for name in list(h.started):
                try:
                    h.stop(name)
                except Exception as error:
                    errors.append(str(error))
            if h.before is not None:
                try:
                    after = h.helper('preservation')
                    local.write(f"preservation-{h.spec['name']}-after.json", after)
                    assert after == h.before, f"installed state changed on {h.spec['name']}"
                except Exception as error:
                    errors.append(str(error))
        result.update(cleanup_errors=errors, installed_state_unchanged=not errors,
                      fixture_processes_stopped=all(not h.started and not h.sessions for h in hosts),
                      duration_seconds=round(time.monotonic() - started, 3))
        if local.before is not None:
            local.write('qualification.json', result)
        if errors:
            raise RuntimeError('; '.join(errors))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
