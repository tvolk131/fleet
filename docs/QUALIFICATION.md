# Fleet qualification

Normal clients, administration and fixture setup all use `fleet` and its JSON interface. The former
`fleet-control-lab` wrapper and direct coordinator/agent protocol are removed. Test scenarios run in
fresh explicit homes and never install or upgrade the normal service.

```sh
cargo build --workspace --all-features --bins --locked
cargo test --workspace --all-features --locked -- --test-threads=2
target/debug/fleet-qualify --help
```

`fleet-qualify` and `fleet-import-baseline` belong to the unpublished `fleet-tools` workspace package.
The workload helper requires `qualification`; this feature also enables the explicit
local partition-file option on `fleet cluster serve`. Product builds use `--features gui` or the
default control-plane features, not `--all-features`.

The runner uses the normal CLI, validates fixture paths, records child identities before signaling
processes and keeps separate role homes. Qualifiers register their own jobs and VM instances for
cleanup. They do not enumerate and stop unrelated workloads. SSH and disposable user-service modes
remain available for physical-machine qualification.

CI tests Rust state machines, redb, iroh, public JSON contracts and the headless iced-m3 GUI on Linux
and macOS. It then fans out the exact built headless binaries into independent workload, networking
and rolling-upgrade scenarios. Workload checks cover checkpoints, backup restore, portable inputs,
retention, history and storage retirement. Networking checks cover VM services, durable guest operations,
local three-replica failures and, on Linux, stock Garage in separate network namespaces. Rolling checks
build explicit future-version fixtures and qualify live work, rollback and incompatible refusal.
The million-receipt snapshot/restore test runs on Linux. Branch pushes with an open PR do not start a
second identical full workflow; PRs, main pushes and manual dispatch are supported.

Fake-driver tests exercise Fleet's contracts without claiming hypervisor or physical network isolation.
Real `vz` VM and multi-machine tests remain separate. CI uploads logs and allowlisted reports, never
fixture homes containing keys or databases. Private local deployment evidence and database backups
remain ignored under `docs/qualification` or the installation's release directories.

See [test coverage](TEST_COVERAGE.md), [baseline import](BASELINE.md), [rolling upgrades](ROLLING_UPGRADES.md)
and [local rollout](LOCAL_ROLLOUT.md) for the corresponding contracts.
