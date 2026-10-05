package tests

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/temporal/environment"
	"go.uber.org/zap/zaptest"
)

const (
	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testCassandraExecutionSchema = "../../../schema/cassandra/temporal/schema.cql"

	testCassandraMaxConnsEnv                  = "CASSANDRA_MAX_CONNS"
	testCassandraMaxExcessShardConnectionsEnv = "CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE"
)

// TODO merge the initialization with existing persistence setup
const (
	testCassandraClusterName = "temporal_cassandra_cluster"

	testCassandraUser               = "temporal"
	testCassandraPassword           = "temporal"
	testCassandraDatabaseNamePrefix = "test_"
	testCassandraDatabaseNameSuffix = "temporal_persistence"
)

type (
	CassandraTestData struct {
		Cfg     *config.Cassandra
		Factory *cassandra.Factory
		Logger  log.Logger
	}
)

func setUpCassandraTest(t testing.TB) (CassandraTestData, func()) {
	return setUpCassandraTestWithConfig(t, nil)
}

func setUpCassandraTestWithHistoryNodeV2Reads(t testing.TB) (CassandraTestData, func()) {
	return setUpCassandraTestWithHistoryNodeMigrationMode(
		t,
		config.CassandraHistoryNodeMigrationModeCanonicalDual,
	)
}

func setUpCassandraTestWithHistoryNodeMigrationMode(
	t testing.TB,
	mode config.CassandraHistoryNodeMigrationMode,
) (CassandraTestData, func()) {
	return setUpCassandraTestWithConfig(t, func(cfg *config.Cassandra) {
		cfg.HistoryNodeMigrationMode = mode
	})
}

func setUpCassandraTestWithConfig(
	t testing.TB,
	configure func(*config.Cassandra),
) (CassandraTestData, func()) {
	var testData CassandraTestData
	testData.Cfg = NewCassandraConfig()
	if configure != nil {
		configure(testData.Cfg)
	}
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	SetUpCassandraDatabase(t, testData.Cfg, testData.Logger)
	SetUpCassandraSchema(t, testData.Cfg, testData.Logger)
	initializeCassandraTargetOnlyLayouts(t, testData.Cfg, testData.Logger)

	testData.Factory = cassandra.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		testData.Logger,
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)

	tearDown := func() {
		testData.Factory.Close()
		TearDownCassandraKeyspace(t, testData.Cfg)
	}

	return testData, tearDown
}

func initializeCassandraTargetOnlyLayouts(t testing.TB, cfg *config.Cassandra, logger log.Logger) {
	t.Helper()
	session := newCassandraTestSession(t, cfg, logger)
	defer session.Close()
	initialize := func(name cassandra.SchemaLayoutName, table string, parameter int64) {
		t.Helper()
		if err := cassandra.InitializeSchemaLayoutTargetOnly(
			context.Background(), session, cfg.Keyspace, name, table, parameter,
		); err != nil {
			t.Fatal(err)
		}
	}
	if cfg.ExecutionMigrationMode == config.CassandraExecutionMigrationModeTargetOnly {
		buckets := cfg.ExecutionStorageBuckets
		if buckets == 0 {
			buckets = 16
		}
		initialize(cassandra.SchemaLayoutExecutions, "executions_v2", int64(buckets))
	}
	if cfg.HistoryNodeMigrationMode == config.CassandraHistoryNodeMigrationModeV2Only {
		initialize(cassandra.SchemaLayoutHistoryNode, "history_node_v2", 0)
	}
	if cfg.HistoryTreeMigrationMode == config.CassandraHistoryTreeMigrationModeTargetOnly {
		initialize(cassandra.SchemaLayoutHistoryTree, "history_tree_v2", 16)
	}
	if cfg.QueueV2MigrationMode == config.CassandraQueueV2MigrationModeTargetOnly {
		span := cfg.QueueV2MessageBucketSpan
		if span == 0 {
			span = cassandra.DefaultQueueV2MessageBucketSpan
		}
		initialize(cassandra.SchemaLayoutQueueV2Metadata, "queues_v2", 64)
		initialize(cassandra.SchemaLayoutQueueV2Messages, "queue_messages_v3", span)
	}
	if cfg.LegacyQueueMigrationMode == config.CassandraLegacyQueueMigrationModeTargetOnly {
		span := cfg.LegacyQueueMessageBucketSize
		if span == 0 {
			span = cassandra.DefaultLegacyQueueV2MessageBucketSize
		}
		initialize(cassandra.SchemaLayoutLegacyQueue, "legacy_queue_v2_messages", span)
		for _, queueType := range []p.QueueType{
			p.NamespaceReplicationQueueType,
			-p.NamespaceReplicationQueueType,
		} {
			if err := cassandra.InitializeEmptyLegacyQueueV2Target(
				context.Background(),
				session,
				queueType,
				span,
			); err != nil {
				t.Fatal(err)
			}
		}
	}
	if cfg.MatchingTaskMigrationMode == config.CassandraMatchingTaskMigrationModeTargetOnly {
		buckets := cfg.MatchingTaskStorageBucketCount
		if buckets == 0 {
			buckets = cassandra.DefaultMatchingTaskStorageBucketCount
		}
		initialize(cassandra.SchemaLayoutMatchingTasks, "tasks_v3", int64(buckets))
		initialize(cassandra.SchemaLayoutMatchingTasksFair, "tasks_v3_fair", int64(buckets))
	}
	if cfg.TaskQueueUserDataMigrationMode == config.CassandraTaskQueueUserDataMigrationModeTargetOnly {
		buckets := cfg.TaskQueueUserDataBucketCount
		if buckets == 0 {
			buckets = cassandra.DefaultTaskQueueUserDataBucketCount
		}
		initialize(cassandra.SchemaLayoutTaskQueueUserData, "task_queue_user_data_v2", int64(buckets))
	}
}

func SetUpCassandraDatabase(t testing.TB, cfg *config.Cassandra, logger log.Logger) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(adminCfg, resolver.NewNoopResolver())
		},
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("unable to create Cassandra session: %v", err)
	}
	defer session.Close()

	if err := cassandra.CreateCassandraKeyspace(
		session,
		cfg.Keyspace,
		1,
		true,
		log.NewNoopLogger(),
	); err != nil {
		t.Fatalf("unable to create Cassandra keyspace: %v", err)
	}
}

func SetUpCassandraSchema(t testing.TB, cfg *config.Cassandra, logger log.Logger) {
	ApplySchemaUpdate(t, cfg, testCassandraExecutionSchema, logger)
}

func ApplySchemaUpdate(t testing.TB, cfg *config.Cassandra, schemaFile string, logger log.Logger) {
	session := newCassandraTestSession(t, cfg, logger)
	defer session.Close()

	schemaPath, err := filepath.Abs(schemaFile)
	if err != nil {
		t.Fatal(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		if err = session.Query(stmt).Exec(); err != nil {
			logger.Error(fmt.Sprintf("Unable to execute statement from file: %s\n  %s", schemaFile, stmt))
			t.Fatal(err)
		}
	}
}

func newCassandraTestSession(
	t testing.TB,
	cfg *config.Cassandra,
	logger log.Logger,
) commongocql.Session {
	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(*cfg, resolver.NewNoopResolver())
		},
		logger,
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TearDownCassandraKeyspace(t testing.TB, cfg *config.Cassandra) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.Keyspace = "system"

	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(adminCfg, resolver.NewNoopResolver())
		},
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		t.Fatalf("unable to create Cassandra session: %v", err)
	}
	defer session.Close()

	if err := cassandra.DropCassandraKeyspace(
		session,
		cfg.Keyspace,
		log.NewNoopLogger(),
	); err != nil {
		t.Fatalf("unable to drop Cassandra keyspace: %v", err)
	}
}

// GetSchemaFiles takes a root directory which contains subdirectories whose names are semantic versions and returns
// the .cql files within. E.g.: //schema/cassandra/temporal/versioned
// Subdirectories are ordered by semantic version, but files within the same subdirectory are in arbitrary order.
// All .cql files are returned regardless of whether they are named in manifest.json.
func GetSchemaFiles(t *testing.T, schemaDir string, logger log.Logger) []string {
	var retVal []string

	versionDirPath := path.Join(schemaDir, "versioned")
	subDirs, err := os.ReadDir(versionDirPath)
	if err != nil {
		t.Fatal(err)
	}

	versionDirNames := make([]string, 0, len(subDirs))
	for _, subDir := range subDirs {
		if !subDir.IsDir() {
			logger.Warn(fmt.Sprintf("Skipping non-directory file: '%s'", subDir.Name()))
			continue
		}
		if _, ve := semver.ParseTolerant(subDir.Name()); ve != nil {
			logger.Warn(fmt.Sprintf("Skipping directory which is not a valid semver: '%s'", subDir.Name()))
		}
		versionDirNames = append(versionDirNames, subDir.Name())
	}

	sort.Slice(versionDirNames, func(i, j int) bool {
		vLeft, err := semver.ParseTolerant(versionDirNames[i])
		if err != nil {
			t.Fatal(err) // Logic error
		}
		vRight, err := semver.ParseTolerant(versionDirNames[j])
		if err != nil {
			t.Fatal(err) // Logic error
		}
		return vLeft.Compare(vRight) < 0
	})

	for _, dir := range versionDirNames {
		vDirPath := path.Join(versionDirPath, dir)
		files, err := os.ReadDir(vDirPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			if !strings.HasSuffix(file.Name(), ".cql") {
				continue
			}
			retVal = append(retVal, path.Join(vDirPath, file.Name()))
		}
	}

	return retVal
}

// NewCassandraConfig returns a new Cassandra config for test
func NewCassandraConfig() *config.Cassandra {
	return &config.Cassandra{
		User:                          testCassandraUser,
		Password:                      testCassandraPassword,
		Hosts:                         environment.GetCassandraAddress(),
		Port:                          environment.GetCassandraPort(),
		MaxConns:                      testCassandraMaxConns(),
		MaxExcessShardConnectionsRate: testCassandraMaxExcessShardConnectionsRate(),
		Keyspace:                      testCassandraDatabaseNamePrefix + shuffle.String(testCassandraDatabaseNameSuffix),
		ConnectTimeout:                30 * time.Second,
	}
}

func testCassandraMaxConns() int {
	maxConns, err := strconv.Atoi(os.Getenv(testCassandraMaxConnsEnv))
	if err != nil {
		return 0
	}
	return maxConns
}

func testCassandraMaxExcessShardConnectionsRate() *float32 {
	rate, err := strconv.ParseFloat(os.Getenv(testCassandraMaxExcessShardConnectionsEnv), 32)
	if err != nil {
		return nil
	}
	rate32 := float32(rate)
	return &rate32
}
