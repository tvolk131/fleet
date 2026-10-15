"""Normal replicated CLI/Python/service contract, with fresh homes and tiny native jobs."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

from fleet import Fleet, FleetError, FleetUnknown

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get('FLEET_TEST_BIN', ROOT / 'target/debug/fleet'))


class ReplicatedFleet(unittest.TestCase):
    @classmethod
    def command(cls, *args):
        result = subprocess.run([str(BIN), *map(str, args)], capture_output=True, text=True, timeout=90)
        if result.returncode:
            raise AssertionError(f'{args}: {result.stderr}\n{result.stdout}')
        return result.stdout

    @classmethod
    def start(cls, role):
        log = open(cls.root / f'{role}.log', 'a')
        cls.logs.append(log)
        child = subprocess.Popen([str(BIN), 'service', 'run', '--config', str(cls.root / f'{role}.json')],
                                 stdout=log, stderr=log)
        cls.processes[role] = child
        return child

    @classmethod
    def stop(cls, role):
        child = cls.processes.pop(role, None)
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix='fleet-normal-client-')
        cls.root = Path(cls.tmp.name)
        cls.logs, cls.processes = [], {}
        cls.cluster = cls.root / 'cluster'
        cls.home = cls.root / 'client'
        cls.worker = cls.root / 'worker'
        cls.command('cluster', 'init', '--home', cls.cluster, '--replicas', '1')
        cls.command('--home', cls.home, 'client', 'import', '--config', cls.cluster / 'client/config.json',
                    '--key', cls.cluster / 'client/node.key')
        cls.command('service', 'configure', '--config', cls.root / 'controller.json',
                    '--controller-home', cls.cluster / 'node-1', '--bootstrap')
        cls.command('cluster', 'prepare-worker', '--home', cls.worker, '--config', cls.home / 'config.json',
                    '--id', 'canary', '--cpu-slots', '1', '--memory-mb', '1024')
        cls.command('service', 'configure', '--config', cls.root / 'worker.json', '--worker-home', cls.worker)
        cls.command('service', 'configure', '--config', cls.root / 'stager.json', '--client-home', cls.home)
        cls.start('controller')
        cls.command('--home', cls.home, 'enroll', cls.worker / 'enroll.json', '--request-id', 'enroll-canary')
        cls.start('worker')
        cls.fleet = Fleet(cls.home, BIN)

    @classmethod
    def tearDownClass(cls):
        try:
            for job in cls.fleet.queue():
                if job['phase'] not in {'succeeded', 'failed', 'cancelled'}:
                    cls.fleet.cancel(job['id'])
        finally:
            for role in list(cls.processes):
                cls.stop(role)
            for log in cls.logs:
                log.close()
            cls.tmp.cleanup()

    def test_queue_inputs_logs_outputs_and_durable_retry(self):
        source = self.root / 'input.txt'
        source.write_text('original input\n')
        job = self.fleet.submit('file job', ['/bin/sh', '-c', 'echo started-once; cp input.txt result.txt'],
                                inputs={'input.txt': source}, publish=['result.txt'],
                                memory_mb=128, request_id='file-request', keep_awake=False,
                                max_seconds=20, settle_seconds=0)
        self.assertEqual(self.fleet.wait(job, timeout=45)['phase'], 'succeeded')
        source.write_text('changed source\n')
        result = self.fleet.retry('file-request')
        self.assertEqual(result['reply']['Accepted']['job'], job)
        self.assertEqual(self.fleet.logs(job)['data'], 'started-once\n')
        destination = self.root / 'capture'
        self.fleet.fetch(job, destination)
        self.assertEqual((destination / 'result.txt').read_text(), 'original input\n')
        self.assertEqual(len([j for j in self.fleet.queue() if j['id'] == job]), 1)
        self.assertTrue(any(r['id'] == 'file-request' for r in self.fleet.requests()))

    def test_dependency_and_cancel_before_placement(self):
        first = self.fleet.submit('first', ['/bin/sh', '-c', 'sleep 1'], memory_mb=128, keep_awake=False, max_seconds=15)
        second = self.fleet.submit('second', ['/bin/sh', '-c', 'echo dependent'], after=[first], memory_mb=128,
                                   keep_awake=False, max_seconds=15)
        self.assertEqual(self.fleet.wait(second, timeout=45)['phase'], 'succeeded')
        self.assertEqual(self.fleet.job(first)['phase'], 'succeeded')
        blocked = self.fleet.submit('no matching host', ['true'], requires=['qualification:no-such-capability'],
                                    memory_mb=128, keep_awake=False, max_seconds=10)
        self.assertEqual(self.fleet.job(blocked)['phase'], 'queued')
        self.fleet.cancel(blocked)
        state = self.fleet.job(blocked)
        self.assertEqual(state['phase'], 'cancelled')
        self.assertIsNone(state['assignment'])

    def test_request_conflict_is_not_a_second_job(self):
        job = self.fleet.submit('one', ['true'], requires=['never'], request_id='conflict', memory_mb=128)
        with self.assertRaises(FleetError):
            self.fleet.submit('different', ['false'], requires=['never'], request_id='conflict', memory_mb=128)
        self.fleet.cancel(job)

    def test_unknown_request_survives_controller_restart(self):
        self.stop('controller')
        try:
            with self.assertRaises(FleetUnknown) as error:
                self.fleet.submit('during outage', ['true'], request_id='outage', requires=['never'], memory_mb=128)
            self.assertEqual(error.exception.request_id, 'outage')
        finally:
            self.start('controller')
        result = self.fleet.retry('outage')
        job = result['reply']['Accepted']['job']
        self.assertEqual(self.fleet.job(job)['phase'], 'queued')
        self.fleet.cancel(job)

    def test_staging_service_delivers_saved_inputs_when_worker_returns(self):
        self.stop('worker')
        source = self.root / 'offline-input.txt'
        source.write_text('saved while offline\n')
        job = self.fleet.submit('offline worker', ['/bin/sh', '-c', 'cp input result'], inputs={'input': source},
                                publish=['result'], memory_mb=128, keep_awake=False, max_seconds=20,
                                request_id='offline-inputs', settle_seconds=0)
        self.assertEqual(self.fleet.job(job)['phase'], 'queued')
        source.unlink()
        self.start('worker')
        self.start('stager')
        try:
            self.assertEqual(self.fleet.wait(job, timeout=60)['phase'], 'succeeded')
        finally:
            self.stop('stager')
        destination = self.root / 'offline-capture'
        self.fleet.fetch(job, destination)
        self.assertEqual((destination / 'result').read_text(), 'saved while offline\n')

    def test_service_configuration_rendering_and_client_identity(self):
        output = self.root / 'fleet-test.plist'
        self.command('service', 'render', '--config', self.root / 'controller.json', '--kind', 'launchd',
                     '--name', 'local.fleet.qualification', '--user', 'testuser', '--user-home', self.root,
                     '--output', output)
        import plistlib
        data = plistlib.loads(output.read_bytes())
        self.assertTrue(data['AbandonProcessGroup'])
        self.assertEqual(data['ProgramArguments'][1:3], ['service', 'run'])
        self.assertTrue(self.fleet.status()['quorum'])
        self.assertEqual((self.home / 'node.key').stat().st_mode & 0o777, 0o600)


if __name__ == '__main__':
    unittest.main()
