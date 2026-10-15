"""fleet from Python: a standard-library client for the coordinator's daemon (`fleet daemon`).

Experiment runners use this instead of SSH and rsync: submit a job (inputs are local files, hashed when queued),
follow its state, read its logs, and fetch its outputs, verified and never overwriting. One JSON request and one JSON
answer per connection to `$FLEET_HOME/daemon.sock` (default `~/.fleet`).

    from fleet import Fleet
    f = Fleet()
    job = f.submit("smoke", ["bin/tool", "--flag"], inputs={"bin/tool": "/path/to/tool"}, publish=["out/*"],
                   gpu=True, tags=["gpu:metal"], dest="runs/smoke")
    f.wait(job)                       # until it finishes; outputs land in runs/smoke as they appear
"""
import json, os, socket, time, uuid
from pathlib import Path

FINISHED = {'succeeded', 'failed', 'cancelled', 'timed_out', 'lost'}


class FleetError(RuntimeError):
    pass


class FleetTransportError(FleetError):
    """A connection failed; an already sent request may have taken effect."""


def socket_path(home):
    """The daemon's socket, as `fleet` computes it: `home/daemon.sock`, or a short path in /tmp when that is too long
    for a Unix socket (FNV-1a of the home's path)."""
    preferred = Path(home) / 'daemon.sock'
    if len(os.fsencode(str(preferred))) < 100:
        return preferred
    h = 0xcbf29ce484222325
    for b in os.fsencode(str(home)):
        h = ((h ^ b) * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return Path(f'/tmp/fleet-{os.getuid()}-{h:016x}.sock')


def expand_inputs(inputs):
    """Job inputs as fleet takes them, file by file: a folder becomes one entry per file inside it."""
    files = {}
    for target, source in inputs.items():
        source = Path(source).resolve()
        if source.is_dir():
            for f in sorted(p for p in source.rglob('*') if p.is_file()):
                files[f'{target.rstrip("/")}/{f.relative_to(source).as_posix()}'] = str(f)
        elif source.is_file():
            files[target] = str(source)
        else:
            raise FleetError(f'input {source}: no such file or folder')
    return files


class Fleet:
    def __init__(self, home=None):
        home = Path(home or os.environ.get('FLEET_HOME') or Path.home() / '.fleet')
        self.socket = socket_path(home)

    def request(self, op, **fields):
        """One request to the daemon; raises FleetError on its errors, or if no daemon is running."""
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(180)
                s.connect(str(self.socket))
                s.sendall((json.dumps({'op': op, **fields}) + '\n').encode())
                data = b''
                while not data.endswith(b'\n'):
                    chunk = s.recv(1 << 16)
                    if not chunk:
                        break
                    data += chunk
            if not data.endswith(b'\n'):
                raise FleetTransportError('daemon disconnected before a complete response')
            answer = json.loads(data)
        except (OSError, ValueError) as e:
            raise FleetTransportError(f'fleet transport at {self.socket}: {e}') from e
        if answer.get('kind') == 'error':
            error = FleetTransportError if answer.get('retryable', False) else FleetError
            raise error(answer['message'])
        return answer

    def machines(self):
        return self.request('machines')['machines']

    def submit(self, name, argv, inputs=None, env=None, publish=None, settle_seconds=30, max_seconds=None,
               keep_awake=True, probes=(), on=None, tags=(), gpu=False, ac_power=False, dest=None, after=(), priority=0,
               threads=1, resume=None, preempt=None, pause_keeps_gpu=False, shutdown_grace_seconds=None,
               idempotency_key=None, vm=None):
        """Queues a job; returns its ID. `inputs` maps paths in the job's folder to local files or folders (a folder
        ships every file in it, at the same place under the path). `probes` are progress probes, for example
        {'jsonl_field': {'file': 'B/metrics.jsonl', 'field': 'iteration', 'total': 384}}. `threads` are claimed from
        the machine's CPU slots; `resume` and `preempt` are the declarations `fleet suspend` and preemption read.
        `idempotency_key` returns the same job for identical retries and rejects changed specifications.
        `shutdown_grace_seconds` defaults to 10 on the native supervisor when omitted.
        `vm` is an instance specification for one disposable macOS VM; inputs, logs and final outputs use
        the normal job API. VM jobs reject native-only pause/suspend/probes/grace options."""
        job = {'name': name, 'argv': [str(a) for a in argv], 'inputs': expand_inputs(inputs or {}),
               'env': {k: str(v) for k, v in (env or {}).items()}, 'publish': list(publish or []),
               'settle_seconds': settle_seconds, 'max_seconds': max_seconds, 'keep_awake': keep_awake,
               'probes': list(probes), 'threads': threads, 'resume': resume, 'preempt': preempt,
               'pause_keeps_gpu': pause_keeps_gpu, 'shutdown_grace_seconds': shutdown_grace_seconds}
        if vm is not None:
            job['vm'] = dict(vm)
        placement = {'machine': on, 'tags': list(tags), 'gpu': gpu, 'ac_power': ac_power}
        if (shutdown_grace_seconds is not None or vm is not None) and idempotency_key is None:
            idempotency_key = f'request/{uuid.uuid4().hex}'
        fields = {} if idempotency_key is None else {'key': idempotency_key}
        answer = self.request('submit_vm_observed' if vm is not None else ('submit' if idempotency_key is None else 'submit_once'), **fields, job=job, placement=placement,
                              dest=str(Path(dest).resolve()) if dest else None, after=list(after), priority=priority)
        return answer['job']

    def find_submission(self, key):
        """Return the existing job ID for a stable submission key, or None."""
        return self.request("find_submission", key=key)["job"]

    def queue(self):
        return self.request('queue')['jobs']

    def job(self, job_id):
        for j in self.queue():
            if j['id'] == job_id:
                return j
        raise FleetError(f'no job {job_id} in the queue')

    def wait(self, job_id, until=None, poll=10, timeout=None, on_change=None):
        """Waits until `until(job)` (default: the job has finished) and returns the job. `on_change(job)` is called
        whenever its state, machine or fetched-file count changes."""
        until = until or (lambda j: j['state'] in FINISHED)
        start, seen = time.time(), None
        while True:
            j = self.job(job_id)
            key = (j['state'], j['machine'], j['fetched_files'])
            if on_change and key != seen:
                on_change(j)
            seen = key
            if until(j):
                return j
            if timeout is not None and time.time() - start > timeout:
                raise FleetError(f'timed out waiting for {job_id} (state {j["state"]})')
            time.sleep(poll)

    def logs(self, job_id, stderr=False, start=0):
        """Log text from byte offset `start`; returns (text, next offset, finished)."""
        a = self.request('logs', job=job_id, stderr=stderr, **{'from': start})
        return a['data'], a['next'], a['done']

    def outputs(self, job_id):
        return self.request('outputs', job=job_id)['files']

    def fetch(self, job_id, dest):
        return self.request('fetch', job=job_id, dest=str(Path(dest).resolve()))['report']

    def cancel(self, job_id):
        self.request('cancel', job=job_id)

    def pause(self, job_id):
        """Freezes a running job where it is (nothing is lost; paused time does not count toward its time limit)."""
        self.request('pause', job=job_id)

    def continue_(self, job_id):
        self.request('continue', job=job_id)

    def plan(self, job_id):
        """What suspending the job would lose now, and when its next checkpoint is due."""
        return self.request('suspend_plan', job=job_id)['plan']

    def suspend(self, job_id, now=False, within_minutes=None):
        """Stops a job at a checkpoint (its next one, or `now`, redoing the work since its latest)."""
        self.request('suspend', job=job_id, now=now,
                     within_seconds=None if within_minutes is None else int(within_minutes * 60))

    def resume(self, job_id, on=None):
        """Queues a suspended job's next segment; returns its ID."""
        return self.request('resume', job=job_id, on=on)['job']

    def move(self, job_id, to, now=False):
        self.request('move', job=job_id, to=to, now=now)

    def instances(self, machine, require_online=True):
        """Worker VM inventory; offline cached inventory is opt-in."""
        a = self.request('instances', machine=machine)
        if require_online and not a['online']:
            raise FleetError(f'worker {machine} is offline')
        return a['instances']

    def instance(self, machine, instance):
        for i in self.instances(machine):
            if i['record']['id'] == instance:
                return i
        raise FleetError(f'unknown instance {instance} on {machine}')

    def instance_create(self, machine, instance, spec):
        """Create a stopped VM; an identical request with the same ID is safe to retry."""
        return self.request('instance_create', machine=machine, id=instance, spec=spec)['instance']

    def instance_set(self, machine, instance, desired, revision=None):
        """Request running/stopped/deleted. Keep the original revision when retrying a lost reply."""
        if revision is None:
            revision = self.instance(machine, instance)['record']['revision']
        return self.request('instance_set', machine=machine, id=instance,
                            desired=desired, revision=revision)['instance']

    def _guest(self, machine, op, **fields):
        request = {op: fields} if fields else op
        response = self.request('instance_guest', machine=machine, request=request)['response']
        return next(iter(response.values()))

    def instance_ready(self, machine, instance, wait=False, timeout=180):
        """Probe the guest command channel (including offline guests), separately from VM power state."""
        deadline = time.monotonic() + timeout
        while True:
            ready = self._guest(machine, 'Ready', id=instance)
            if ready['ready'] or not wait:
                return ready
            if time.monotonic() >= deadline:
                raise FleetError(f'guest not ready: {ready["message"]}')
            time.sleep(1)

    def instance_exec(self, machine, instance, argv, *, operation, revision=None,
                      cwd=None, env=None, timeout_seconds=600):
        """Start noninteractive guest argv; returns a durable operation record, without waiting.
        Save `operation` before calling; reconnect with instance_operation/wait/logs, never a new ID.
        A client disconnect does not cancel execution. No host shell or shared folders are involved."""
        if revision is None:
            revision = self.instance(machine, instance)['record']['revision']
        return self._guest(machine, 'Start', id=instance, operation=operation, revision=revision,
                           spec={'kind': 'exec', 'argv': [str(a) for a in argv], 'cwd': cwd,
                                 'env': {k: str(v) for k, v in (env or {}).items()},
                                 'timeout_seconds': timeout_seconds})

    def instance_operation(self, machine, instance, operation):
        return self._guest(machine, 'Get', id=instance, operation=operation)

    def instance_operations(self, machine, instance):
        """Bounded summaries; use instance_operation for the full request and any error."""
        return self._guest(machine, 'List', id=instance)

    def instance_logs(self, machine, instance, operation, stderr=False, start=0):
        """Return (bytes, next byte offset, finished). Decode with an incremental decoder if needed."""
        a = self._guest(machine, 'Logs', id=instance, operation=operation,
                        stderr=stderr, **{'from': start})
        return bytes(a['data']), a['next'], a['done']

    def instance_wait(self, machine, instance, operation, poll=.25, timeout=None, on_output=None):
        """Wait for a terminal result, including failure/interruption. Client timeout does not cancel it.
        Optional on_output(stderr: bool, data: bytes) receives both logs, drained through completion."""
        deadline = None if timeout is None else time.monotonic() + timeout
        offsets = [0, 0]
        while True:
            op = self.instance_operation(machine, instance, operation)
            drained = True
            if on_output:
                streams = [True] if op['spec']['kind'] == 'download' else [False, True]
                for stderr in streams:
                    data, offsets[stderr], done = self.instance_logs(
                        machine, instance, operation, stderr, offsets[stderr])
                    if data:
                        on_output(stderr, data)
                    drained &= done
            if op['state'] in {'succeeded', 'failed', 'interrupted'} and drained:
                return op
            if deadline is not None and time.monotonic() >= deadline:
                raise FleetError(f'timed out waiting for operation {operation}; it may still be running')
            time.sleep(poll)

    def instance_upload(self, machine, instance, source, path, *, operation, revision=None):
        """Stage one regular local file and start its guest upload. Wait with instance_wait.
        `path` is absolute inside the guest; conflicting existing files are never overwritten."""
        if revision is None:
            revision = self.instance(machine, instance)['record']['revision']
        a = self.request('instance_upload', machine=machine, id=instance, operation=operation,
                          revision=revision, source=os.path.abspath(source), path=str(path))
        return a['response']['Operation']

    def instance_download(self, machine, instance, path, dest, *, operation,
                          revision=None, max_bytes=1 << 30, timeout=None):
        """Capture a guest regular file, wait, and fetch it to an explicit local file without overwriting.
        Reconnect to a captured download with instance_wait then instance_fetch, even after the VM stops."""
        if revision is None:
            revision = self.instance(machine, instance)['record']['revision']
        self._guest(machine, 'Start', id=instance, operation=operation, revision=revision,
                    spec={'kind': 'download', 'path': str(path), 'max_bytes': max_bytes})
        op = self.instance_wait(machine, instance, operation, timeout=timeout)
        if op['state'] != 'succeeded':
            raise FleetError(f'download {operation} {op["state"]}: {op["error"]}')
        return self.instance_fetch(machine, instance, operation, dest)

    def instance_fetch(self, machine, instance, operation, dest):
        """Fetch an already successful download operation; the VM need not be running."""
        return self.request('instance_download', machine=machine, id=instance,
                            operation=operation, dest=os.path.abspath(dest))

    def instance_storage(self, machine):
        """Read-only storage inventory. APFS allocated blocks can be shared, not reclaimable totals."""
        return self._guest(machine, 'Storage')
