# Temporal performance suite

`temporalperf` is a self-contained black-box Temporal workload generator and performance suite. It can start a real Temporal server from a standard server config file or target an already running server. It calibrates each workflow-profile and payload-size pair independently, then measures end-to-end workflow latency at 50%, 80%, and 90% of that pair's sustainable throughput. It also submits a fixed number of workflows without rate limiting and measures how quickly the system drains them.

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

In managed-server mode, every calibration, steady-state, and fixed-N sample follows the same lifecycle:

1. If configured, execute `-reset-command` and wait for it to finish.
2. Start `-server-binary` with the unchanged `-config-file` and wait for frontend health.
3. Connect to Temporal, start in-process SDK workers, and run a rate-controlled warmup.
4. Run the measured sample through the same client, workers, and task queues.
5. Validate that every submitted workflow completed and that no workflow failed.
6. Stop the Temporal server gracefully.
7. If configured, execute `-cleanup-command`. If it is omitted, the reset command is used for cleanup too. Cleanup runs even when reset, server startup, warmup, or measurement fails.

Reset and cleanup commands receive these environment variables:

- `TEMPORALPERF_ADDRESS`
- `TEMPORALPERF_NAMESPACE`
- `TEMPORALPERF_CONFIG_FILE`
- `TEMPORALPERF_PROFILE`
- `TEMPORALPERF_PAYLOAD_BYTES`
- `TEMPORALPERF_STAGE`
- `TEMPORALPERF_TRIAL`
- `TEMPORALPERF_ARTIFACT_DIR`

The Temporal config is passed directly to the real server, so its Cassandra/Scylla, MySQL, PostgreSQL, visibility, TLS, consistency, and connection settings are used without a benchmark-specific copy. The optional reset command owns only database-specific lifecycle in managed mode. It must leave equivalent initialized schemas for every sample and return only when the database is stable. It must not start Temporal. Omitting reset and cleanup intentionally reuses an initialized database, so results include database growth and sample-order effects.

Without `-config-file`, the suite retains external-server mode. In that mode the reset command must also start Temporal and wait for frontend health, as before. This supports remote and multi-node deployments.

## Calibration

Calibration begins at `-initial-rps` and multiplies offered load by `-growth-factor`. A point is sustainable when:

- all workflows complete;
- no workflow fails; and
- achieved throughput is at least `-success-ratio` of offered throughput.

The last sustainable achieved throughput, capped at its offered rate, is the case maximum. Reaching `-max-rps` is recorded as a calibration ceiling rather than silently treating the configured limit as system saturation.

Each warmup or measured sample is bounded by `-sample-timeout`. Paced samples stop launching at the configured warmup or measurement boundary, then use the remaining sample timeout to drain workflows and clean up incomplete executions.

## Example

Build the real server and benchmark command:

```bash
make temporal-server
go build -o /tmp/temporalperf ./cmd/tools/temporalperf
```

Run against schemas already initialized by the standard Temporal schema tools:

```bash
/tmp/temporalperf \
  -config-file ./config/development-cass-es.yaml \
  -server-binary ./temporal-server \
  -namespace temporal-perf \
  -warmup 30s \
  -measurement 1m \
  -trials 3 \
  -batch-workflows 10000 \
  -output-dir /tmp/temporalperf-scylla
```

Unless `-address` is set explicitly, managed mode derives the local frontend address from the config's `services.frontend.rpc` settings. For reproducible clean-store comparisons, supply backend-specific `-reset-command` and `-cleanup-command` hooks. Server output for each sample is saved in that sample's `server.log`.

`suite.json` is updated atomically after every completed profile/payload case. It contains median throughput and p50/p95/p99 workflow latency across trials at each target. Each sample directory contains the server, configured hook, warmup, and measurement logs plus the raw load result and generic runtime metadata.
