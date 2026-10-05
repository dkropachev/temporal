//go:build integration

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

func BenchmarkCassandraMatchingTaskQueueTargetOnly(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()
	recreateMatchingTasksV3Schema(b, testData)

	for _, tc := range []struct {
		name string
		fair bool
	}{
		{name: "classic_target_only"},
		{name: "fair_target_only", fair: true},
	} {
		store, err := testData.Factory.NewTaskStoreWithMatchingTaskMigration(
			tc.fair,
			config.CassandraMatchingTaskMigrationModeTargetOnly,
			16,
		)
		require.NoError(b, err)
		manager := p.NewTaskManager(store, serialization.NewSerializer())
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCassandraMatchingTaskQueue(b, manager, tc.fair)
		})
	}
}
