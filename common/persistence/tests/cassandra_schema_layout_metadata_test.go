//go:build integration

package tests

import (
	"sync"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/cassandra"
)

func TestCassandraSchemaLayoutMetadataLifecycle(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()
	ApplySchemaUpdate(
		t,
		testData.Cfg,
		"../../../schema/cassandra/temporal/versioned/v1.16/schema_layout_metadata.cql",
		testData.Logger,
	)
	session := newCassandraTestSession(t, testData.Cfg, testData.Logger)
	defer session.Close()
	store := cassandra.NewSchemaLayoutMetadataStore(session)
	generation, err := gocql.RandomUUID()
	require.NoError(t, err)
	spec := cassandra.SchemaLayoutSpec{
		Name:               cassandra.SchemaLayoutExecutions,
		Generation:         generation,
		Version:            2,
		ImmutableParameter: 16,
	}

	preparing, err := store.InitializePreparing(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, cassandra.SchemaLayoutAuthorityPreparing, preparing.AuthorityState)
	require.Equal(t, int64(0), preparing.Epoch)
	_, err = store.RequireTargetOnly(t.Context(), spec)
	var authorityErr *cassandra.SchemaLayoutAuthorityError
	require.ErrorAs(t, err, &authorityErr)

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	results := make(chan error, 2)
	for range 2 {
		go func() {
			defer waitGroup.Done()
			_, transitionErr := store.CompareAndSwapAuthority(
				t.Context(),
				preparing,
				cassandra.SchemaLayoutAuthorityTargetReady,
			)
			results <- transitionErr
		}()
	}
	waitGroup.Wait()
	close(results)
	var successes, conflicts int
	for transitionErr := range results {
		if transitionErr == nil {
			successes++
			continue
		}
		var conflictErr *cassandra.SchemaLayoutConflictError
		require.ErrorAs(t, transitionErr, &conflictErr)
		conflicts++
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)

	targetOnly, err := store.MarkTargetOnly(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, cassandra.SchemaLayoutAuthorityTargetOnly, targetOnly.AuthorityState)
	require.Equal(t, int64(2), targetOnly.Epoch)
	loaded, err := store.RequireTargetOnly(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, targetOnly, loaded)

	mismatch := spec
	mismatch.ImmutableParameter++
	_, err = store.RequireTargetOnly(t.Context(), mismatch)
	var mismatchErr *cassandra.SchemaLayoutMismatchError
	require.ErrorAs(t, err, &mismatchErr)

	retired, err := store.MarkRetired(t.Context(), spec)
	require.NoError(t, err)
	require.Equal(t, cassandra.SchemaLayoutAuthorityRetired, retired.AuthorityState)
	_, err = store.RequireTargetOnly(t.Context(), spec)
	require.ErrorAs(t, err, &authorityErr)
}
