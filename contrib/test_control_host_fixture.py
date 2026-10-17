"""The destructive fixture operations must remain confined to their own homes and processes."""
import json
from pathlib import Path
import os
import sys
import subprocess
import tempfile
import unittest

import control_host_fixture
from control_host_fixture import main
from qualify_normal_multihost import LaunchdHost


class FixtureSafeguards(unittest.TestCase):
    def test_paths_and_unrelated_pid_are_refused(self):
        with tempfile.TemporaryDirectory(prefix='fleet-control-helper-', dir='/tmp') as parent:
            root = Path(parent) / 'run'
            main({'root': str(root), 'op': 'create'})
            for destination in ['../escaped', '/tmp/escaped']:
                with self.assertRaises(AssertionError):
                    main({'root': str(root), 'op': 'write', 'path': destination, 'data': 'eA=='})
            (root / 'processes').mkdir()
            (root / 'processes/foreign.json').write_text(json.dumps({'pid': os.getpid(), 'identity': 'not this process'}))
            with self.assertRaises(AssertionError):
                main({'root': str(root), 'op': 'stop', 'name': 'foreign'})
            with self.assertRaises(FileExistsError):
                main({'root': str(root), 'op': 'create'})

    def test_service_role_outside_fixture_is_refused(self):
        with tempfile.TemporaryDirectory(prefix='fleet-control-helper-', dir='/tmp') as parent:
            root = Path(parent) / 'run'
            main({'root': str(root), 'op': 'create'})
            config = root / 'service.json'
            for settings in [
                {'controller_home': '/tmp/not-this-fixture'},
                {'controller_home': str(root / 'controller'), 'worker_home': str(Path.home() / '.fleet')},
                {'client_home': '../escape'},
                {'controller_home': str(root / 'controller'), 'storage_home': '/tmp/not-this-fixture'},
                {},
            ]:
                config.write_text(json.dumps(settings))
                with self.assertRaises(AssertionError):
                    main({'root': str(root), 'op': 'spawn', 'name': 'invalid',
                          'command': [sys.executable, 'service', 'run', '--config', str(config)]})
                # Validate before calling launchctl, so this also runs on Linux CI.
                host = LaunchdHost({'name': 'test', 'ssh': [], 'binary': sys.executable}, root)
                with self.assertRaises(AssertionError):
                    host.spawn('invalid', 'service', 'run', '--config', str(config))
            self.assertFalse((root / 'processes/invalid.json').exists())

    def test_only_recorded_fixture_child_is_stopped(self):
        with tempfile.TemporaryDirectory(prefix='fleet-control-helper-', dir='/tmp') as parent:
            root = Path(parent) / 'run'
            main({'root': str(root), 'op': 'create'})
            request = {'root': str(root), 'name': 'child'}
            main({**request, 'op': 'spawn', 'command': [sys.executable, '-c', 'import time; time.sleep(30)', '--home', str(root / 'worker')]})
            try:
                self.assertTrue(main({**request, 'op': 'alive'}))
            finally:
                self.assertTrue(main({**request, 'op': 'stop'}))
            self.assertFalse(main({**request, 'op': 'alive'}))

    def test_held_launch_session_lives_until_its_child_is_stopped(self):
        with tempfile.TemporaryDirectory(prefix='fleet-control-helper-', dir='/tmp') as parent:
            root = Path(parent) / 'run'
            main({'root': str(root), 'op': 'create'})
            request = {'root': str(root), 'name': 'held'}
            with subprocess.Popen([sys.executable, control_host_fixture.__file__], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, text=True) as session:
                session.stdin.write(json.dumps({**request, 'op': 'spawn', 'hold_session': True,
                    'command': [sys.executable, '-c', 'import time; time.sleep(30)', '--home', str(root / 'worker')]}))
                session.stdin.close()
                record = json.loads(session.stdout.readline())
                try:
                    self.assertGreater(record['pid'], 0)
                    self.assertIsNone(session.poll())
                    self.assertTrue(main({**request, 'op': 'alive'}))
                finally:
                    main({**request, 'op': 'stop'})
                self.assertEqual(session.wait(timeout=5), 0)
                self.assertFalse(main({**request, 'op': 'alive'}))


if __name__ == '__main__':
    unittest.main()
