package workflow

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	commonnexus "go.temporal.io/server/common/nexus"
)

func testOperationIdentity() commonnexus.OperationIdentity {
	return commonnexus.OperationIdentity{
		NamespaceID:      "namespace-id",
		WorkflowID:       "workflow-id",
		RunID:            "run-id",
		ScheduledEventID: 42,
		RequestID:        "request-id",
	}
}

func TestCompletionFromOperationIdentity_CHASM(t *testing.T) {
	t.Parallel()
	completion, err := CompletionFromOperationIdentity(testOperationIdentity())
	require.NoError(t, err)
	require.Equal(t, "request-id", completion.GetRequestId())
	require.Empty(t, completion.GetNamespaceId())
	require.Nil(t, completion.GetRef())

	var ref persistencespb.ChasmComponentRef
	require.NoError(t, ref.Unmarshal(completion.GetComponentRef()))
	require.Equal(t, "namespace-id", ref.GetNamespaceId())
	require.Equal(t, "workflow-id", ref.GetBusinessId())
	require.Equal(t, "run-id", ref.GetRunId())
	require.Equal(t, chasm.WorkflowArchetypeID, ref.GetArchetypeId())
	require.Equal(t, []string{operationsFieldName, strconv.FormatInt(42, 10)}, ref.GetComponentPath())
}

func TestOperationIdentityFromCompletion_CHASM(t *testing.T) {
	t.Parallel()
	completion, err := CompletionFromOperationIdentity(testOperationIdentity())
	require.NoError(t, err)
	id, err := OperationIdentityFromCompletion(completion)
	require.NoError(t, err)
	require.Equal(t, testOperationIdentity(), id)
}

func TestOperationIdentityFromCompletion_CHASMErrors(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		path []string
	}{
		{name: "too short", path: []string{operationsFieldName}},
		{name: "wrong field", path: []string{"Callbacks", "1"}},
		{name: "non-numeric id", path: []string{operationsFieldName, "nope"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			refBytes, err := (&persistencespb.ChasmComponentRef{ComponentPath: tc.path}).Marshal()
			require.NoError(t, err)
			_, err = OperationIdentityFromCompletion(&tokenspb.NexusOperationCompletion{ComponentRef: refBytes})
			require.Error(t, err)
		})
	}
}
