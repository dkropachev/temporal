## Using the cassandra schema tool
This package contains the tooling for temporal cassandra operations.

## For localhost development
For the very first time run:
``` 
make
```

then run:
``` 
make install-schema-cass
```
to create schema in your `cassandra` instance.

## For production

### Create the binaries
- Run `make`
- You should see an executable `temporal-cassandra-tool`

### Do one time database creation and schema setup for a new cluster
This uses Cassandra's SimpleStratagey for replication. For production, we recommend using a replication factor of 3 with NetworkTopologyStrategy.

```
temporal-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
```

See https://www.ecyrd.com/cassandracalculator for an easy way to determine how many nodes and what replication factor you will want to use.  Note that Temporal by default uses `LOCAL_QUORUM` and `LOCAL_SERIAL` for read and write consistency.

```
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned -- upgrades your schema to the latest version
```

### Update schema as part of a release
You can only upgrade to a new version after the initial setup done above.

```
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned -v x.x    -- executes the upgrade to version x.x
```

### History node online migration

The history primary-key migration uses explicit Temporal configuration modes. See
[`docs/development/scylla-cassandra-performance.md`](../../docs/development/scylla-cassandra-performance.md#history-upgrade-from-v1)
for the required rollout order.

```bash
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal recreate-history-node-v2 --confirm-source-rebuild
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal backfill-history-node-v2 \
  --checkpoint-file ./history-node-v2-pass-1.json
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal recreate-history-node-v1 --confirm-v1-rebuild
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal backfill-history-node-v1 \
  --checkpoint-file ./history-node-v1-pass-1.json
```

Reuse a checkpoint path to resume a failed pass and use a new path for each deliberate complete pass. The defaults scan
4096 token ranges with 16-row pages and 16 concurrent writes; tune them with `--token-ranges`, `--page-size`, and
`--concurrency`. Backfills require Murmur3 partitioning and revalidate the source and target table generations after
the copy.
