# Reproduce the recovery report

The report has two separate signals: synchronized testnet nodes alternate
between appearing behind in the dashboard after one node returns, and traffic
through other listeners reportedly drops while CPU rises. The latter has not
been established by reproducing the dashboard defect.

Run the complete local scenario on Windows:

```powershell
./scripts/reproduce-recovery.ps1 -Repeat 3
./scripts/reproduce-recovery.ps1 -NoRestart -Repeat 3
```

Pass `-TargetDirectory <cargo-target-directory>` to reuse a build directory,
or `-OutputDirectory <new-evidence-directory>` to choose where captures go.
The PowerShell runner needs PowerShell 7 and Cargo on PATH.
The second command is a control with the same workload and timing but no
node restart. Mainnet readers offer 60 MiB/s combined, close to the reported
500 Mbps scale. Use `-MiBPerSecond 0` for an unbounded backfill stress run.
Add `-Release` to test optimized binaries; the initial build takes longer.
Use `-TimingScale 10` for the reported real intervals: reconnect at 3000 ms,
status and monitoring at 5000 ms, and two blocks per second. That run lasts
roughly two minutes. The default uses one tenth of those times.

On any supported platform, the underlying fixture runs with:

```sh
cargo test --locked --test recovery_repro -- --ignored --nocapture
```

The load fixture is ignored by default to keep the regular suite quick. Its
final assertion fails when synchronized nodes show dashboard lag, as on
v0.7.0, and passes with the recovery fix. The PowerShell runner records
that verdict and completes the remaining repetitions. A missing summary means
the scenario failed to complete, not that the reported defect was reproduced.

The fixture runs the actual router binary against Spring-mode SHiP doubles:

1. Start 14 listeners, two upstreams each, with distinct chain IDs per network.
2. Open 18 client WebSockets: two readers backfill 64 KiB block payloads on
   `network-0`; sixteen lightweight readers follow the other networks.
3. Advance both testnet heads identically throughout every phase. Record a
   baseline, including any pre-existing sample-age differences.
4. Stop the primary of `network-1`, keeping mainnet backfill running. Existing
   testnet clients must fail over without a close, duplicate or skipped block.
5. Restart that primary on the same endpoint, away from its backup's polling
   boundary. Advance both testnet nodes identically. Raw status samples should
   differ as their polls alternate, while measured dashboard lag stays zero.

Polling is scaled to 500 ms, reconnect to 300 ms, with two blocks every 100 ms, to exercise the
same sample-age mechanism in seconds. Counts match the reported deployment;
the per-listener session distribution and synthetic payloads are assumptions.
These are local protocol doubles, not real nodeos or Hyperion instances.

Each run saves `metadata.json` (PID, dashboard URL and workload),
`samples.jsonl` (phase, full router status, actual mock heads, poll counters
and reader progress), `summary.json` (lag counts and mainnet throughput per
phase), router logs, final metrics and session details. Raw sample differences
are recorded separately from the measured lag used by the fixed dashboard.
The Windows runner also
saves process CPU time, resident memory, thread and handle counts in
`process-cpu.jsonl`. On Linux, the fixture records the router's `/proc` CPU,
resident memory and thread counters directly. CPU time is cumulative process seconds; differences
divided by wall seconds give CPU cores used, not host-normalized percentages.

The direct Rust command prints the live dashboard URL. It lasts about ten
to fifteen seconds after setup. Use the saved raw status snapshots to inspect every poll
after the process exits. No production config or runtime code is changed.

For the smallest two-node regression, without background listeners or load:

```sh
cargo test --locked --test control_api restarted_synced_nodes_do_not_alternate_dashboard_lag -- --nocapture
```

## Baseline observations

On the unmodified v0.7.0 runtime, three paced Windows release runs reproduced
both directions of false head lag. Three matched no-restart controls did not
reproduce alternating lag; two had a transient one-sided raw sample difference.
An additional release run at the reported 3000/5000/5000 ms intervals also
reproduced both directions. All eighteen reader sockets stayed open and
delivered consecutive blocks. Mainnet throughput remained about 60 MiB/s
before, during and after recovery (59.96/59.99/59.98 MiB/s at real intervals).

No consistent sustained CPU increase or cross-listener slowdown was observed.
The performance incident remains unconfirmed. These local protocol doubles
do not establish production Hyperion behavior or reproduce the upstream
software, ABI sizes, host CPU budget and complete configuration.

The fix measures a conservative lag against live peer samples already known
when each status request starts. It avoids treating poll age as lag, while
preserving a positive reading when a fresh response remains behind an already
known peer head. Differences within one polling interval can go undetected;
the raw stored block number and coverage ranges remain available for inspection.

With the fix, the same Windows release scenario at real intervals reported
zero false-lag samples. The raw samples still differed in both directions
(12 samples for one node, 39 for the other), so the polling offset remained
exercised. All eighteen readers stayed connected and delivered consecutive
blocks; throughput was 59.96/59.97/60.00 MiB/s across the three phases.
