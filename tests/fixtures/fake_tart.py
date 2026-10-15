#!/usr/bin/python3
"""Process-level Tart test double; never invokes a hypervisor or touches the real Tart home."""
import fcntl, json, os, signal, sys, time
from pathlib import Path

args=sys.argv[1:]
if args==['--version']:
    print('2.40.1'); sys.exit(0)
root=Path(os.environ['TART_HOME']); (root/'vms').mkdir(parents=True,exist_ok=True)
with (root/'history').open('a') as f: f.write(' '.join(args)+'\n')
op=args[0]
if op=='list':
    result=[]
    for vm in (root/'vms').iterdir():
        state='stopped'
        if (vm/'pid').exists():
            try: os.kill(int((vm/'pid').read_text()),0);state='running'
            except (ProcessLookupError, FileNotFoundError): pass
        result.append({'Name':vm.name,'State':state})
    print(json.dumps(result))
elif op=='clone':
    if (root/'slow-clone').exists():time.sleep(30)
    vm=root/'vms'/args[2];vm.mkdir();(vm/'disk').write_text('initial')
elif op=='set':
    assert (root/'vms'/args[1]).is_dir()
elif op=='run':
    if (root/'fail-run').exists():
        print('The maximum supported number of virtual machines has been reached.', file=sys.stderr)
        sys.exit(42)
    assert '--net-softnet' in args and '--net-softnet-block' in args
    assert not any(a.startswith(('--dir','--net-bridged','--net-host','--net-softnet-allow','--net-softnet-expose')) for a in args)
    vm=root/'vms'/args[1]
    with (vm/'lock').open('w') as f:
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        (vm/'pid.tmp').write_text(str(os.getpid()))
        (vm/'pid.tmp').replace(vm/'pid')
        def stop(*_):
            (vm/'pid').unlink(missing_ok=True);sys.exit(0)
        signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
        if (root/'fail-after-lock').exists():
            time.sleep(3)
            print('Late virtual machine capacity error',file=sys.stderr)
            (vm/'pid').unlink(missing_ok=True)
            sys.exit(42)
        while True:time.sleep(.1)
elif op in ['exec','stop']:
    if op=='exec' and args[1]=='-i':
        assert (root/'vms'/args[2]/'pid').exists()
        assert args[3:6]==['/usr/bin/python3','-I','-c']
        # Test callers use only fixture-local paths. This branch exercises the
        # real fixed guest helper without requiring macOS virtualization in CI.
        # Disposable VM job paths live only under this fixture, not the host /tmp namespace.
        prefix = "/tmp/" + args[2] + "/work"
        args[-1] = args[-1].replace(prefix, str(root/"vms"/args[2]/"guest-work"))
        os.execv(args[3], args[3:])
    if op=='exec':
        assert args[2:]==['/usr/bin/sudo','-n','/sbin/shutdown','-h','now']
        if (root/'refuse-shutdown').exists():sys.exit(1)
    pid=root/'vms'/args[1]/'pid'
    if pid.exists():os.kill(int(pid.read_text()),signal.SIGTERM)
    for _ in range(50):
        if not pid.exists():break
        time.sleep(.02)
elif op=='delete':
    import shutil
    shutil.rmtree(root/'vms'/args[1])
else:raise Exception(args)
