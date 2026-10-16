"""JSON CLI/service contract, with fresh homes and tiny native jobs."""
from functools import partial
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from cli_fixture import api, api_result, wait_for

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
        cls.request = staticmethod(partial(api_result, BIN, cls.home))

    def test_storage_reports_are_timestamped_and_offline_is_explicit(self):
        status = wait_for(lambda: self.request(op='storage'), lambda s: any(
            w['worker'] == 'canary' and w['report'] for w in s['workers']))
        report = next(w['report'] for w in status['workers'] if w['worker'] == 'canary')
        self.assertGreater(report['observed_at'], 0)
        self.assertIn('blob_payload_bytes', report)
        self.assertIn('retention', status)
        self.stop('worker')
        try:
            offline = self.request(op='storage')['workers'][0]
            self.assertIsNone(offline['report'])
            self.assertTrue(offline['error'])
        finally:
            self.start('worker')

    @classmethod
    def tearDownClass(cls):
        try:
            for job in cls.request(op='queue'):
                if job['phase'] not in {'succeeded', 'failed', 'cancelled'}:
                    cls.request(op='cancel', id=job['id'], request_id=f"cleanup-{job['id']}",
                                expected_revision=job['revision'])
                    wait_for(lambda: cls.request(op='job', id=job['id']),
                             lambda j: j['phase'] in {'succeeded', 'failed', 'cancelled'})
        finally:
            for role in list(cls.processes):
                cls.stop(role)
            for log in cls.logs:
                log.close()
            cls.tmp.cleanup()

    def test_queue_inputs_logs_outputs_and_durable_retry(self):
        source = self.root / 'input.txt'
        source.write_text('original input\n')
        job = 'file-job'
        self.request(op='submit', id=job, request_id='file-request', memory_mb=128,
                     job={'name': 'file job', 'argv': ['/bin/sh', '-c', 'echo started-once; cp input.txt result.txt'],
                          'inputs': {'input.txt': str(source)}, 'publish': ['result.txt'],
                          'keep_awake': False, 'max_seconds': 20, 'settle_seconds': 0})
        wait_for(lambda: self.request(op='job', id=job), lambda j: j['phase'] == 'succeeded', timeout=45)
        source.write_text('changed source\n')
        result = self.request(op='retry', request_id='file-request')
        self.assertEqual(result['reply']['Accepted']['job'], job)
        self.assertEqual(self.request(op='logs', id=job)['Logs']['data'], 'started-once\n')
        self.assertEqual([o['path'] for o in self.request(op='outputs', id=job)['Outputs']], ['result.txt'])
        destination = self.root / 'capture'
        self.request(op='fetch', id=job, destination=str(destination))
        self.assertEqual((destination / 'result.txt').read_text(), 'original input\n')
        self.assertEqual(len([j for j in self.request(op='queue') if j['id'] == job]), 1)
        self.assertTrue(any(r['id'] == 'file-request' for r in self.request(op='requests')))
        (destination / 'result.txt').write_text('user edited this export\n')
        answer = api(BIN, self.home, dict(op='fetch', id=job, destination=str(destination)))
        self.assertFalse(answer['ok'], answer)
        self.assertEqual((destination / 'result.txt').read_text(), 'user edited this export\n')
        conflict = api(BIN, self.home, dict(op='submit', id=job, request_id='file-request', memory_mb=128,
                                           job={'name': 'different payload', 'argv': ['false']}))
        self.assertFalse(conflict['ok'], conflict)
        self.assertEqual(len(self.request(op='attempts', id=job)), 1)

    def test_executable_inputs_and_log_cursor_survive_service_restarts(self):
        script = self.root / 'canary.sh'
        gate = self.root / 'release-canary'
        script.write_text('#!/bin/sh\necho before-restart\nwhile [ ! -f "$1" ]; do sleep .1; done\necho after-restart\n')
        script.chmod(0o700)
        job = 'restart-logs'
        self.request(op='submit', id=job, request_id='restart-logs-submit', memory_mb=128,
                     job={'name': 'executable log canary', 'argv': ['./bin/canary', str(gate)],
                          'inputs': {'bin/canary': str(script)}, 'keep_awake': False, 'max_seconds': 60})
        try:
            wait_for(lambda: self.request(op='job', id=job), lambda j: j['phase'] == 'running')
            first = wait_for(lambda: self.request(op='logs', id=job)['Logs'],
                             lambda log: log['data'] == 'before-restart\n')
            self.stop('controller')
            self.stop('worker')
            self.start('controller')
            self.start('worker')
            # Every read still goes through the logical control plane; no direct scheduler fallback.
            wait_for(lambda: self.request(op='job', id=job), lambda j: j['phase'] == 'running')
            gate.write_text('go\n')
            wait_for(lambda: self.request(op='job', id=job), lambda j: j['phase'] == 'succeeded')
            tail = self.request(op='logs', id=job, **{'from': first['next']})['Logs']
            self.assertEqual(tail['data'], 'after-restart\n')
            self.assertTrue(tail['done'])
            self.assertEqual(self.request(op='logs', id=job)['Logs']['data'], 'before-restart\nafter-restart\n')
            self.assertEqual(len(self.request(op='attempts', id=job)), 1)
        finally:
            gate.write_text('go\n')
            for role in ('controller', 'worker'):
                if role not in self.processes:
                    self.start(role)

    def test_dependency_and_cancel_before_placement(self):
        self.request(op='submit', id='first', request_id='first-submit', memory_mb=128,
                     job={'name': 'first', 'argv': ['/bin/sh', '-c', 'sleep 1'], 'keep_awake': False, 'max_seconds': 15})
        self.request(op='submit', id='second', request_id='second-submit', memory_mb=128, after=['first'],
                     job={'name': 'second', 'argv': ['/bin/sh', '-c', 'echo dependent'], 'keep_awake': False, 'max_seconds': 15})
        wait_for(lambda: self.request(op='job', id='second'), lambda j: j['phase'] == 'succeeded', timeout=45)
        self.assertEqual(self.request(op='job', id='first')['phase'], 'succeeded')
        self.request(op='submit', id='blocked', request_id='blocked-submit', memory_mb=128,
                     requires=['qualification:no-such-capability'],
                     job={'name': 'no matching host', 'argv': ['true'], 'keep_awake': False, 'max_seconds': 10})
        state = self.request(op='job', id='blocked')
        self.assertEqual(state['phase'], 'queued')
        self.request(op='cancel', id='blocked', request_id='blocked-cancel', expected_revision=state['revision'])
        state = self.request(op='job', id='blocked')
        self.assertEqual(state['phase'], 'cancelled')
        self.assertIsNone(state['assignment'])

    def test_request_conflict_is_not_a_second_job(self):
        request = dict(op='submit', id='conflict-job', request_id='conflict', requires=['never'], memory_mb=128,
                       job={'name': 'one', 'argv': ['true']})
        self.request(**request)
        request['job'] = {'name': 'different', 'argv': ['false']}
        answer = api(BIN, self.home, request)
        self.assertFalse(answer['ok'])
        self.assertFalse(answer['outcome_unknown'])
        self.assertEqual(answer['request_id'], 'conflict')
        state = self.request(op='job', id='conflict-job')
        self.request(op='cancel', id='conflict-job', request_id='conflict-cancel', expected_revision=state['revision'])

    def test_unknown_request_survives_controller_restart(self):
        self.stop('controller')
        try:
            answer = api(BIN, self.home, dict(op='submit', id='outage-job', request_id='outage',
                                            requires=['never'], memory_mb=128,
                                            job={'name': 'during outage', 'argv': ['true']}))
            self.assertFalse(answer['ok'])
            self.assertTrue(answer['outcome_unknown'])
            self.assertEqual(answer['request_id'], 'outage')
        finally:
            self.start('controller')
        result = self.request(op='retry', request_id='outage')
        job = result['reply']['Accepted']['job']
        state = self.request(op='job', id=job)
        self.assertEqual(state['phase'], 'queued')
        self.request(op='cancel', id=job, request_id='outage-cancel', expected_revision=state['revision'])

    def test_committed_rejection_is_a_reply_and_retry_preserves_it(self):
        request = dict(op='submit', id='rejected-job', request_id='rejected-submit', memory_mb=128,
                       after=['missing-dependency'], job={'name': 'rejected', 'argv': ['true']})
        answer = api(BIN, self.home, request)
        self.assertTrue(answer['ok'])
        self.assertIn('Rejected', answer['result']['reply'])
        retried = api(BIN, self.home, dict(op='retry', request_id='rejected-submit'))
        self.assertEqual(retried, answer)
        self.assertFalse(any(j['id'] == 'rejected-job' for j in self.request(op='queue')))

    def test_staging_service_delivers_saved_inputs_when_worker_returns(self):
        self.stop('worker')
        source = self.root / 'offline-input.txt'
        source.write_text('saved while offline\n')
        job = 'offline-job'
        self.request(op='submit', id=job, request_id='offline-inputs', memory_mb=128,
                     job={'name': 'offline worker', 'argv': ['/bin/sh', '-c', 'cp input result'],
                          'inputs': {'input': str(source)}, 'publish': ['result'], 'keep_awake': False,
                          'max_seconds': 20, 'settle_seconds': 0})
        self.assertEqual(self.request(op='job', id=job)['phase'], 'queued')
        source.unlink()
        self.start('worker')
        self.start('stager')
        try:
            wait_for(lambda: self.request(op='job', id=job), lambda j: j['phase'] == 'succeeded')
        finally:
            self.stop('stager')
        destination = self.root / 'offline-capture'
        self.request(op='fetch', id=job, destination=str(destination))
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
        self.assertTrue(self.request(op='status')['quorum'])
        self.assertEqual((self.home / 'node.key').stat().st_mode & 0o777, 0o600)


if __name__ == '__main__':
    unittest.main()
