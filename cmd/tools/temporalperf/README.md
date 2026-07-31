# Temporal performance suite

`temporalperf` runs the same black-box Temporal workload against any persistence backend. It calibrates each workflow-profile and payload-size pair independently, then measures end-to-end workflow latency at 50%, 80%, and 90% of that pair's sustainable throughput. It also submits a fixed number of workflows without rate limiting and measures how quickly the system drains them.

The default matrix contains:

| Profile | Sequential activity steps |
| --- | ---: |
| `tiny` | 1 |
| `medium` | 5 |
| `big` | 20 |

| Payload bucket | Bytes |
| --- | ---: |
| Small | 128 |
| Typical | 4,096 |
| Large | 65,536 |
| Maximum practical test bucket | 1,048,576 |

The payload is used for the workflow input and every activity input. Four buckets are the maximum accepted by the suite.

## Lifecycle

Every calibration, steady-state, and fixed-N sample follows the same lifecycle:

1. Execute `-reset-command` and wait for it to finish.
2. Start workers and run a rate-controlled warmup against the same task queues and workload shape.
3. Run the measured sample.
4. Validate that every submitted workflow completed and that no workflow failed.
5. Execute `-cleanup-command`. If it is omitted, the reset command is used for cleanup too. Cleanup runs even when reset, warmup, or measurement fails.

Reset and cleanup commands receive these environment variables:

- `TEMPORALPERF_ADDRESS`
- `TEMPORALPERF_NAMESPACE`
- `TEMPORALPERF_PROFILE`
- `TEMPORALPERF_PAYLOAD_BYTES`
- `TEMPORALPERF_STAGE`
- `TEMPORALPERF_TRIAL`
- `TEMPORALPERF_ARTIFACT_DIR`

The reset command owns database-specific lifecycle. It must recreate equivalent schemas and configuration, start Temporal, wait for frontend health, and return only when the system is stable. This keeps scenario generation identical for Scylla, Cassandra, MySQL, and PostgreSQL without embedding database control into the load generator.

## Calibration

Calibration begins at `-initial-rps` and multiplies offered load by `-growth-factor`. A point is sustainable when:

- all workflows complete;
- no workflow fails; and
- achieved throughput is at least `-success-ratio` of offered throughput.

The last sustainable achieved throughput is the case maximum. Reaching `-max-rps` is recorded as a calibration ceiling rather than silently treating the configured limit as system saturation.

## Example

Build both commands first:

```bash
go build -o /tmp/scyllaload ./cmd/tools/scyllaload
go build -o /tmp/temporalperf ./cmd/tools/temporalperf
```

Then run the suite with lifecycle scripts for the selected backend:

```bash
/tmp/temporalperf \
  -scyllaload /tmp/scyllaload \
  -address 127.0.0.1:7233 \
  -namespace temporal-perf \
  -reset-command './perf/reset-scylla.sh' \
  -cleanup-command './perf/cleanup-scylla.sh' \
  -warmup 30s \
  -measurement 1m \
  -trials 3 \
  -batch-workflows 10000 \
  -output-dir /tmp/temporalperf-scylla
```

`suite.json` is updated atomically after every completed profile/payload case. It contains median throughput and p50/p95/p99 workflow latency across trials at each target. Each sample directory contains reset, cleanup, warmup, and measurement logs plus the raw `scyllaload` result and environment metadata.
