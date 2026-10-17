#!/usr/bin/env python3
"""Build immutable previous source, an explicitly incompatible fixture, and this candidate.

Sequential builds share dependency artifacts; the final target/debug/fleet is always
the candidate. No production executable has a runtime compatibility override.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = a.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    previous = json.loads((root / 'contrib/upgrade_baseline.json').read_text())
    target = root / 'target'
    env = {**os.environ, 'CARGO_TARGET_DIR': str(target)}

    def build(source, name):
        # Package outputs have the same name across source revisions. Force Fleet itself
        # to rebuild; dependencies remain cached. Never copy a stale shared target binary.
        subprocess.run(['cargo', 'clean', '-p', 'fleet'], cwd=source, env=env, check=True)
        subprocess.run(['cargo', 'build', '--all-features', '--bin', 'fleet', '--locked'],
                       cwd=source, env=env, check=True)
        shutil.copy2(target / 'debug/fleet', output / name)

    with tempfile.TemporaryDirectory(prefix='fleet-upgrade-build-') as scratch:
        scratch = Path(scratch)
        baseline = scratch / 'baseline'
        baseline.mkdir()
        archive = subprocess.check_output(['git', 'archive', previous['revision']], cwd=root)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            for member in tar.getmembers():
                assert member.isfile() or member.isdir(), 'source archive contains a link/device'
                assert not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
            tar.extractall(baseline)
        build(baseline, 'fleet-baseline')
        future = scratch / 'unsupported'
        future.mkdir()
        # Only build inputs are copied; never include keys, databases, or qualification homes.
        shutil.copytree(root / 'src', future / 'src')
        for name in ('Cargo.toml', 'Cargo.lock'):
            shutil.copy2(root / name, future / name)
        version = future / 'src/control/version.rs'
        text = version.read_text()
        assert 'pub const MIN: u32 = 1;' in text and 'pub const MAX: u32 = 2;' in text
        version.write_text(text.replace('pub const MIN: u32 = 1;', 'pub const MIN: u32 = 3;')
                          .replace('pub const MAX: u32 = 2;', 'pub const MAX: u32 = 4;'))
        build(future, 'fleet-unsupported')
        build(root, 'fleet-candidate')
    result = {'baseline': previous, 'unsupported_is_test_fixture': True,
              'sha256': {name: hashlib.sha256((output / name).read_bytes()).hexdigest()
                         for name in ('fleet-baseline', 'fleet-candidate', 'fleet-unsupported')}}
    assert len(set(result['sha256'].values())) == 3, 'qualification binaries must be distinct'
    (output / 'builds.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
