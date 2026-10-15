"""Test/qualification helpers for the JSON CLI, not a supported client library.

Callers supply the actual wire request, including IDs and revisions. No operation
methods, input expansion, retries, or request generation belong here.
"""
import json
import subprocess
import time


def api(binary, home, request):
    """Return the unmodified envelope so tests can assert errors and unknown outcomes."""
    process = subprocess.run([str(binary), '--home', str(home), 'api'],
                             input=json.dumps(request), text=True, capture_output=True, timeout=90)
    assert process.returncode == 0, (process.returncode, process.stderr, process.stdout)
    answer = json.loads(process.stdout)
    assert isinstance(answer, dict) and isinstance(answer.get('ok'), bool), answer
    return answer


def api_result(binary, home, **request):
    """Assert a successful request when a scenario is not testing the error envelope."""
    answer = api(binary, home, request)
    assert answer['ok'], answer
    result = answer['result']
    if isinstance(result, dict) and isinstance(result.get('reply'), dict):
        assert not {'Rejected', 'RecoveryUnknown', 'Expired'}.intersection(result['reply']), answer
    return result


def wait_for(call, predicate, timeout=60, poll=.2):
    """Bounded polling for fixture assertions; exceptions are not retried."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = call()
        if predicate(last):
            return last
        time.sleep(poll)
    raise AssertionError(f'timed out waiting for fixture state: {last}')
