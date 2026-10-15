"""Fleet's replicated client. Uses the normal Rust client through `fleet api`.

Each mutation has a stable request ID and a durable prepared payload in the client
home. FleetUnknown means inspect/retry that ID, never blindly create another job.
Bulk data stays on workers; this adapter has no scheduling authority.
"""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


class FleetError(RuntimeError):
    pass


class FleetUnknown(FleetError):
    def __init__(self, message, request_id=None):
        super().__init__(message)
        self.request_id = request_id


class Fleet:
    def __init__(self, home=None, binary=None):
        self.home = str(Path(home or os.environ.get('FLEET_HOME') or Path.home() / '.fleet-client'))
        self.binary = str(binary or os.environ.get('FLEET_BIN') or 'fleet')

    def request(self, op, **fields):
        request = dict(op=op, **fields)
        try:
            process = subprocess.run([self.binary, '--home', self.home, 'api'],
                                     input=json.dumps(request), text=True, capture_output=True)
            answer = json.loads(process.stdout)
        except (OSError, ValueError) as error:
            raise FleetUnknown(f'Fleet client did not return a complete response: {error}',
                               fields.get('request_id')) from error
        if not answer.get('ok'):
            message = answer.get('error', process.stderr or 'Fleet request failed')
            if answer.get('outcome_unknown'):
                raise FleetUnknown(message, answer.get('request_id'))
            raise FleetError(message)
        result = answer['result']
        if isinstance(result, dict):
            reply = result.get('reply', {})
            if isinstance(reply, dict) and 'Rejected' in reply:
                raise FleetError(reply['Rejected']['reason'])
            if isinstance(reply, dict) and 'RecoveryUnknown' in reply:
                raise FleetUnknown('Request history absent after emergency recovery; it was not reexecuted',
                                   fields.get('request_id'))
        return result

    @staticmethod
    def _id(value=None):
        return value or f'request-{uuid.uuid4().hex}'

    def submit(self, name, argv, *, inputs=None, env=None, publish=(), worker=None,
               threads=1, memory_mb=1024, gpu=False, requires=(), after=(), priority=0,
               vm=None, checkpoint=None, probes=(), max_seconds=None, settle_seconds=30,
               keep_awake=True, shutdown_grace_seconds=None, request_id=None, job_id=None):
        request_id = self._id(request_id)
        # Derivation makes retrying the same Python call with a caller-supplied ID
        # stable even if the caller did not retain the generated logical job ID.
        job_id = job_id or f'job-{uuid.uuid5(uuid.NAMESPACE_OID, request_id).hex}'
        files = {}
        for target, source in (inputs or {}).items():
            source = Path(source).resolve()
            if source.is_dir():
                for file in sorted(p for p in source.rglob('*') if p.is_file()):
                    files[f'{target.rstrip("/")}/{file.relative_to(source).as_posix()}'] = str(file)
            elif source.is_file():
                files[target] = str(source)
            else:
                raise FleetError(f'Input does not exist: {source}')
        job = dict(name=name, argv=[str(a) for a in argv], inputs=files,
                   env={k: str(v) for k, v in (env or {}).items()}, publish=list(publish),
                   threads=threads, vm=vm, checkpoint=checkpoint, probes=list(probes), max_seconds=max_seconds,
                   settle_seconds=settle_seconds, keep_awake=keep_awake,
                   shutdown_grace_seconds=shutdown_grace_seconds)
        self.request('submit', request_id=request_id, id=job_id, job=job, worker=worker,
                     memory_mb=memory_mb, gpu=gpu, priority=priority, after=list(after),
                     requires=list(requires))
        return job_id

    def retry(self, request_id):
        return self.request('retry', request_id=request_id)

    def requests(self):
        return self.request('requests')

    def status(self):
        return self.request('status')

    def workers(self):
        return self.request('workers')

    def queue(self):
        return self.request('queue')

    def job(self, job_id):
        return self.request('job', id=job_id)

    def cancel(self, job_id, *, request_id=None, revision=None):
        if revision is None:
            revision = self.job(job_id)['revision']
        return self.request('cancel', request_id=self._id(request_id), id=job_id,
                            expected_revision=revision)

    def checkpoints(self, job_id):
        return self.request('checkpoints', id=job_id)

    def fetch_checkpoint(self, checkpoint_id, destination):
        return self.request('fetch_checkpoint', id=checkpoint_id, destination=str(Path(destination).resolve()))

    def attempts(self, job_id):
        return self.request('attempts', id=job_id)

    def suspend(self, job_id, *, checkpoint='latest', request_id=None, revision=None):
        """Stop at the selected verified checkpoint; subsequent work will be redone."""
        if revision is None:
            revision = self.job(job_id)['revision']
        return self.request('suspend', request_id=self._id(request_id), id=job_id,
                            checkpoint=checkpoint, expected_revision=revision)

    def resume(self, job_id, *, request_id=None, revision=None):
        if revision is None:
            revision = self.job(job_id)['revision']
        return self.request('resume', request_id=self._id(request_id), id=job_id,
                            expected_revision=revision)

    def stage(self, job_id=None):
        return self.request('stage', id=job_id)

    def wait(self, job_id, *, timeout=None, poll=1, on_change=None):
        start = time.monotonic()
        previous = None
        while True:
            job = self.job(job_id)
            if on_change and job != previous:
                on_change(job)
            previous = job
            if job['phase'] in {'succeeded', 'failed', 'cancelled'}:
                return job
            if timeout is not None and time.monotonic() - start >= timeout:
                raise TimeoutError(f'Waiting for {job_id} timed out; the job is not cancelled')
            time.sleep(poll)

    def logs(self, job_id, *, stderr=False, offset=0, maximum=65536, attempt=None):
        return self.request('logs', id=job_id, attempt=attempt, stderr=stderr, **{'from': offset, 'max': maximum})['Logs']

    def outputs(self, job_id, *, attempt=None):
        return self.request('outputs', id=job_id, attempt=attempt)['Outputs']

    def fetch(self, job_id, destination, *, attempt=None):
        return self.request('fetch', id=job_id, attempt=attempt, destination=str(Path(destination).resolve()))

    def events(self, worker, *, since=0, wait_ms=0):
        return self.request('data', worker=worker,
                            request={'Events': {'since': since, 'wait_ms': wait_ms}})['Events']

    def instances(self):
        return self.request('instances')

    def create_instance(self, instance_id, worker, spec, *, request_id=None):
        return self.request('instance_create', request_id=self._id(request_id),
                            id=instance_id, worker=worker, spec=spec)

    def set_instance(self, instance_id, desired, revision, *, request_id=None):
        return self.request('instance_set', request_id=self._id(request_id), id=instance_id,
                            expected_revision=revision, desired=desired)

    def drain(self, worker, *, drain=True, request_id=None):
        return self.request('drain', request_id=self._id(request_id), worker=worker, drain=drain)
