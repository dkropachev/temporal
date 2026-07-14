package nexusoperations

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	commonnexus "go.temporal.io/server/common/nexus"
)

func testHSMCompletion() *tokenspb.NexusOperationCompletion {
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "workflow-id",
		RunId:       "run-id",
		RequestId:   "request-id",
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{Type: OperationMachineType, Id: "42"}},
		},
	}
}

func TestOperationIdentityFromCompletion_HSM(t *testing.T) {
	t.Parallel()
	id, err := OperationIdentityFromCompletion(testHSMCompletion())
	require.NoError(t, err)
	require.Equal(t, commonnexus.OperationIdentity{
		NamespaceID:      "namespace-id",
		WorkflowID:       "workflow-id",
		RunID:            "run-id",
		ScheduledEventID: 42,
		RequestID:        "request-id",
	}, id)
}

func TestCompletionFromOperationIdentity_HSM(t *testing.T) {
	t.Parallel()
	id := commonnexus.OperationIdentity{
		NamespaceID:      "namespace-id",
		WorkflowID:       "workflow-id",
		RunID:            "run-id",
		ScheduledEventID: 42,
		RequestID:        "request-id",
	}
	completion := CompletionFromOperationIdentity(id)
	require.Equal(t, "namespace-id", completion.GetNamespaceId())
	require.Equal(t, "workflow-id", completion.GetWorkflowId())
	require.Equal(t, "run-id", completion.GetRunId())
	require.Equal(t, "request-id", completion.GetRequestId())
	require.Empty(t, completion.GetComponentRef())
	require.Len(t, completion.GetRef().GetPath(), 1)
	require.Equal(t, OperationMachineType, completion.GetRef().GetPath()[0].GetType())
	require.Equal(t, strconv.FormatInt(42, 10), completion.GetRef().GetPath()[0].GetId())
	// Non-nil so the completion handler's reset fallback can zero the transition counts safely.
	require.NotNil(t, completion.GetRef().GetMachineInitialVersionedTransition())
	require.NotNil(t, completion.GetRef().GetMachineLastUpdateVersionedTransition())
}

func TestOperationIdentityRoundTrip_HSM(t *testing.T) {
	t.Parallel()
	id, err := OperationIdentityFromCompletion(testHSMCompletion())
	require.NoError(t, err)
	got, err := OperationIdentityFromCompletion(CompletionFromOperationIdentity(id))
	require.NoError(t, err)
	require.Equal(t, id, got)
}

func TestOperationIdentityFromCompletion_HSMErrors(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		ref  *persistencespb.StateMachineRef
	}{
		{name: "no operation key", ref: &persistencespb.StateMachineRef{}},
		{name: "non-numeric id", ref: &persistencespb.StateMachineRef{Path: []*persistencespb.StateMachineKey{{Type: OperationMachineType, Id: "nope"}}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OperationIdentityFromCompletion(&tokenspb.NexusOperationCompletion{Ref: tc.ref})
			require.Error(t, err)
		})
	}
}
