#!/usr/bin/env python3
"""End-to-end test of the historical contrib/fleet_legacy.py: a real agent and a real daemon on this machine, in scratch FLEET_HOMEs, talking
over local iroh with relays and DNS disabled. Run from the repository after `cargo build --release`:

    python3 contrib/test_fleet_legacy.py
"""
import json, os, socket, subprocess, sys, tempfile, time, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'contrib'))
from fleet_legacy import Fleet, FleetError, FleetTransportError  # noqa: E402

BIN = Path(os.environ.get('FLEET_LEGACY_TEST_BIN', ROOT / 'target/debug/fleet-legacy'))


class FleetPy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='fleet-py-')
        root = Path(cls.tmp.name)
        cls.agent_home, cls.home = root / 'agent', root / 'coordinator'
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        cls.env = {**os.environ, 'FLEET_NETWORK': 'local'}
        cls.env.pop('FLEET_PORT', None)
        run = lambda home, *args: subprocess.run([str(BIN), *args], env={**cls.env, 'FLEET_HOME': str(home)},
                                                  check=True, capture_output=True, text=True).stdout.strip()
        coordinator = run(cls.home, 'id')
        run(cls.agent_home, 'allow', coordinator)
        agent = run(cls.agent_home, 'id', '--role', 'agent')
        run(cls.home, 'machine', 'add', 'local', agent, '--addr', f'127.0.0.1:{port}')
        cls.logs = [open(root / f'{cmd}.log', 'w') for cmd in ('agent', 'daemon')]
        start = lambda home, cmd, log: subprocess.Popen([str(BIN), cmd],
            env={**cls.env, 'FLEET_HOME': str(home), **({'FLEET_PORT': str(port)} if cmd == 'agent' else {})},
                                                         stdout=subprocess.DEVNULL, stderr=log)
        cls.start = staticmethod(start)
        cls.processes = [start(cls.agent_home, 'agent', cls.logs[0]), start(cls.home, 'daemon', cls.logs[1])]
        cls.fleet = Fleet(cls.home)
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                if any(m['online'] and m['status'] for m in cls.fleet.machines()):
                    return
            except FleetError:
                pass
            time.sleep(1)
        raise RuntimeError('the agent never came online')

    @classmethod
    def tearDownClass(cls):
        for p in cls.processes:
            p.terminate()
            p.wait()
        for log in cls.logs:
            log.close()
        cls.tmp.cleanup()

    def test_a_job_runs_and_its_outputs_arrive(self):
        script = Path(self.tmp.name) / 'make.sh'
        script.write_text('#!/bin/sh\nmkdir -p out && echo "$1" > out/answer.txt && echo made\n')
        script.chmod(0o755)
        dest = Path(self.tmp.name) / 'results'
        job = self.fleet.submit('py job', ['bin/make.sh', '42'], inputs={'bin/make.sh': script}, publish=['out/*'],
                                settle_seconds=0, keep_awake=False, dest=dest)
        done = self.fleet.wait(job, poll=0.5, timeout=60)
        self.assertEqual(done['state'], 'succeeded')
        self.fleet.wait(job, until=lambda j: j['fetched_files'] == 1, poll=0.5, timeout=60)
        self.assertEqual((dest / 'out/answer.txt').read_text(), '42\n')
        text, _, finished = self.fleet.logs(job)
        self.assertEqual((text, finished), ('made\n', True))
        self.assertEqual([o['path'] for o in self.fleet.outputs(job)], ['out/answer.txt'])

    def test_a_folder_input_ships_every_file_and_threads_are_claimed(self):
        folder = Path(self.tmp.name) / 'point'
        (folder / 'model').mkdir(parents=True)
        (folder / 'state.json').write_text('{"iteration": 7}\n')
        (folder / 'model/weights.bin').write_bytes(b'\x00\x01')
        dest = Path(self.tmp.name) / 'listed'
        job = self.fleet.submit('py folder', ['/bin/sh', '-c', 'mkdir -p out && find start -type f | sort > out/files'
                                              ' && echo "$RAYON_NUM_THREADS" > out/threads'],
                                inputs={'start': folder}, threads=2, settle_seconds=0, keep_awake=False, dest=dest,
                                publish=['out/*'])
        self.assertEqual(self.fleet.wait(job, poll=0.5, timeout=60)['state'], 'succeeded')
        self.fleet.wait(job, until=lambda j: j['fetched_files'] == 2, poll=0.5, timeout=60)
        self.assertEqual((dest / 'out/files').read_text(), 'start/model/weights.bin\nstart/state.json\n')
        self.assertEqual((dest / 'out/threads').read_text(), '2\n')
        with self.assertRaises(FleetError):
            self.fleet.submit('missing', ['true'], inputs={'x': Path(self.tmp.name) / 'nothing-here'})

    def test_errors_come_back_as_exceptions(self):
        with self.assertRaises(FleetError):
            self.fleet.submit('nowhere', ['true'], on='no-such-machine')

    def test_submission_reconnect_and_daemon_restart(self):
        f = self.fleet
        key = 'python-test/lost-response'
        request = {'op': 'submit_once', 'key': key,
                   'job': {'name': 'lost response', 'argv': ['/bin/echo', 'once'], 'keep_awake': False}}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(str(f.socket))
            sock.sendall((json.dumps(request) + '\n').encode())
            # Drop the connection without reading the response.
        deadline = time.time() + 30
        job = None
        while job is None and time.time() < deadline:
            job = f.find_submission(key)
            time.sleep(.05)
        self.assertIsNotNone(job)
        cls = type(self)
        cls.processes[1].terminate()
        cls.processes[1].wait(timeout=30)
        cls.processes[1] = cls.start(cls.home, 'daemon', cls.logs[1])
        deadline = time.time() + 30
        while True:
            try:
                self.assertEqual(f.find_submission(key), job)
                break
            except FleetError:
                if time.time() > deadline: raise
                time.sleep(.1)
        self.assertEqual(f.request(**request)['job'], job)
        request['job']['argv'] = ['/bin/echo', 'different']
        with self.assertRaisesRegex(FleetError, 'different specification'):
            f.request(**request)
        self.assertEqual(f.wait(job, poll=.1, timeout=30)['state'], 'succeeded')
        self.assertEqual(len([j for j in f.queue() if j['name'] == 'lost response']), 1)

    def test_typed_connection_errors_are_distinct_from_semantic_errors(self):
        from unittest.mock import MagicMock, patch
        for retryable in [False, True]:
            connection = MagicMock()
            connection.recv.return_value = (json.dumps({'kind': 'error', 'message': 'example',
                                                       'retryable': retryable}) + '\n').encode()
            connection.__enter__.return_value = connection
            with patch('fleet_legacy.socket.socket', return_value=connection):
                with self.assertRaises(FleetError) as caught:
                    self.fleet.request('fetch', job='example', dest='/tmp/example')
                self.assertEqual(isinstance(caught.exception, FleetTransportError), retryable)

    def test_custom_shutdown_grace_through_python(self):
        job = self.fleet.submit('grace', ['/usr/bin/python3', '-c',
            'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print("ready", flush=True); time.sleep(60)'],
            shutdown_grace_seconds=1, keep_awake=False)
        self.fleet.wait(job, until=lambda j: j['state'] == 'running', poll=.1, timeout=30)
        start = time.monotonic()
        self.fleet.cancel(job)
        done = self.fleet.wait(job, poll=.1, timeout=30)
        self.assertEqual(done['state'], 'cancelled')
        self.assertEqual(done['exit']['signal'], 9)
        self.assertLess(time.monotonic() - start, 6)

    def test_vm_guest_operations_files_and_reconnect(self):
        import hashlib
        f, cls = self.fleet, type(self)
        root = Path(self.tmp.name)
        tart = root / 'fake-tart'
        tart.write_text((ROOT / 'tests/fixtures/fake_tart.py').read_text())
        tart.chmod(0o700)
        softnet = root / 'softnet'
        softnet.write_text('#!/bin/sh\necho softnet 0.24.0-test\n')
        softnet.chmod(0o700)
        (self.agent_home / 'instances.json').write_text(json.dumps({
            'tart': str(tart), 'softnet': str(softnet),
            'images': {'macos': 'registry.test/macos@sha256:' + 'a'*64},
            'memory_budget_mb': 8192, 'disk_budget_gb': 200,
            'min_free_disk_gb': 5, 'block_cidrs': []}))
        vm = 'python-guest'
        spec = dict(image='macos', cpus=2, memory_mb=4096, disk_gb=50,
                    network='offline', gpu='exclusive')
        f.instance_create('local', vm, spec)

        def state(expected):
            deadline = time.monotonic() + 30
            while True:
                i = f.instance('local', vm)
                if i['observed']['state'] == expected and i['record']['revision'] == i['observed']['revision']:
                    return i
                if time.monotonic() > deadline:
                    raise AssertionError(i)
                time.sleep(.1)

        state('stopped')
        self.assertFalse(f.instance_ready('local', vm)['ready'])
        f.instance_set('local', vm, 'running')
        state('running')
        f.instance_ready('local', vm, wait=True, timeout=30)
        try:
            source, guest, dest = root/'input', root/'guest payload;$(literal)', root/'download'
            source.write_bytes(bytes(range(256))*(40*1024))  # 10 MiB exceeds the JSON message limit.
            up = f.instance_upload('local', vm, source, str(guest), operation='upload')
            self.assertEqual(up['id'], 'upload')
            self.assertEqual(f.instance_wait('local', vm, 'upload', timeout=40)['state'], 'succeeded')
            result = f.instance_download('local', vm, str(guest), dest, operation='download', timeout=40)
            self.assertEqual(result['bytes'], source.stat().st_size)
            self.assertEqual(hashlib.sha256(dest.read_bytes()).digest(), hashlib.sha256(source.read_bytes()).digest())
            f.instance_fetch('local', vm, 'download', dest)  # Identical destination is safe to repeat.
            dest.write_text('keep this')
            with self.assertRaisesRegex(FleetError, 'overwrite'):
                f.instance_fetch('local', vm, 'download', dest)
            self.assertEqual(dest.read_text(), 'keep this')
            symlink = root/'symlink-download'
            symlink.symlink_to(source)
            with self.assertRaises(FleetError):
                f.instance_fetch('local', vm, 'download', symlink)
            with self.assertRaises(FleetError):
                f.instance_upload('local', vm, symlink, str(guest), operation='symlink-upload')

            marker = root/'once'
            argv = ['/usr/bin/python3', '-c',
                    f"import time; open({str(marker)!r},'ab').write(b'x'); print('started',flush=True); time.sleep(6); print('done')"]
            f.instance_exec('local', vm, argv, operation='survive', timeout_seconds=20)
            deadline = time.monotonic() + 20
            while b'started' not in f.instance_logs('local', vm, 'survive')[0]:
                if time.monotonic() > deadline: self.fail('no initial guest output')
                time.sleep(.1)
            for p in cls.processes:
                p.terminate(); p.wait(timeout=30)
            cls.processes[:] = [cls.start(cls.agent_home, 'agent', cls.logs[0]),
                                cls.start(cls.home, 'daemon', cls.logs[1])]
            deadline = time.monotonic() + 30
            while True:
                try:
                    f.instance_operation('local', vm, 'survive')
                    break
                except FleetError:
                    if time.monotonic() > deadline: raise
                    time.sleep(.2)
            output = []
            op = f.instance_wait('local', vm, 'survive', timeout=30,
                                 on_output=lambda stderr, data: output.append((stderr, data)))
            self.assertEqual(op['state'], 'succeeded')
            self.assertEqual(b''.join(data for err,data in output if not err), b'started\ndone\n')
            self.assertEqual(f.instance_exec('local', vm, argv, operation='survive', timeout_seconds=20)['state'], 'succeeded')
            self.assertEqual(marker.read_bytes(), b'x')
            with self.assertRaisesRegex(FleetError, 'different request'):
                f.instance_exec('local', vm, ['/bin/true'], operation='survive')
            self.assertEqual({op['id'] for op in f.instance_operations('local', vm)}, {'upload','download','survive'})
            self.assertEqual(f.instance_storage('local')['reserved_capacity_gb'], 50)
        finally:
            f.instance_set('local', vm, 'stopped')
            state('stopped')
            f.instance_fetch('local', vm, 'download', root/'after-stop')
            f.instance_set('local', vm, 'deleted')
            state('deleted')


    def test_vm_jobs_use_the_queue_and_cli(self):
        f = self.fleet
        root = Path(self.tmp.name)
        tart = root / 'fake-tart'
        tart.write_text((ROOT / 'tests/fixtures/fake_tart.py').read_text())
        tart.chmod(0o700)
        softnet = root / 'softnet'
        softnet.write_text('#!/bin/sh\necho softnet 0.24.0-test\n')
        softnet.chmod(0o700)
        (self.agent_home / 'instances.json').write_text(json.dumps({
            'tart': str(tart), 'softnet': str(softnet),
            'images': {'macos': 'registry.test/macos@sha256:' + 'a'*64},
            'memory_budget_mb': 8192, 'disk_budget_gb': 200,
            'min_free_disk_gb': 5, 'block_cidrs': []}))
        spec = dict(image='macos', cpus=2, memory_mb=4096, disk_gb=50,
                    network='offline', gpu='exclusive')
        dest = root/'vm-job-result'
        arguments = dict(vm=spec, threads=2, dest=dest, publish=['out/*'],
                         idempotency_key='python/vm-job',
                         probes=[{'log_regex': {'name':'status', 'pattern':'vm-(.*)'}}])
        argv = ['/bin/sh', '-c', 'mkdir -p out; echo isolated > out/result; echo vm-log']
        job = f.submit('Python VM job', argv, **arguments)
        self.assertEqual(f.submit('Python VM job', argv, **arguments), job)
        self.assertEqual(f.wait(job, poll=.2, timeout=90)['state'], 'succeeded')
        f.wait(job, until=lambda j:j['fetched_files']==1, poll=.2, timeout=30)
        self.assertEqual((dest/'out/result').read_text(), 'isolated\n')
        self.assertEqual(f.logs(job)[0], 'vm-log\n')
        self.assertEqual(f.job(job)['vm']['image'], 'macos')
        ready = f.wait(job, until=lambda j:j.get('progress') and j.get('vm_status',{}).get('phase')=='finished', poll=.2, timeout=30)
        self.assertEqual(ready['progress'][0]['reading']['text']['text'], 'log')
        # The CLI routes a VM job through a request older daemons reject.
        definition = root/'vm-job.json'
        definition.write_text(json.dumps({'name':'CLI VM job', 'argv':['/bin/echo','cli-vm'], 'vm':spec, 'max_seconds':30}))
        result = subprocess.run([str(BIN),'submit',str(definition),'--on','local','--json'],
            env={**self.env,'FLEET_HOME':str(self.home)},capture_output=True,text=True,check=True)
        submitted=json.loads(result.stdout)
        job=submitted['job'] if isinstance(submitted,dict) else submitted
        self.assertEqual(f.wait(job,poll=.2,timeout=90)['state'],'succeeded')


if __name__ == '__main__':
    unittest.main(verbosity=2)
