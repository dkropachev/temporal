package workflow

import (
	"fmt"
	"strconv"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	commonnexus "go.temporal.io/server/common/nexus"
)

// operationsFieldName is the first segment of a Nexus operation's CHASM component path (the
// scheduled event ID, the map key, is the second). It must stay in sync with the name of the
// Workflow.Operations field in workflow.go: if that field is renamed, update this constant.
const operationsFieldName = "Operations"

// OperationIdentityFromCompletion extracts the framework-agnostic operation identity from a
// CHASM-format Nexus completion token, whose ComponentRef addresses the operation at
// ["Operations", "<scheduledEventID>"] under the workflow.
func OperationIdentityFromCompletion(completion *tokenspb.NexusOperationCompletion) (commonnexus.OperationIdentity, error) {
	var componentRef persistencespb.ChasmComponentRef
	if err := componentRef.Unmarshal(completion.GetComponentRef()); err != nil {
		return commonnexus.OperationIdentity{}, fmt.Errorf("failed to unmarshal component ref: %w", err)
	}
	scheduledEventID, err := scheduledEventIDFromComponentPath(componentRef.GetComponentPath())
	if err != nil {
		return commonnexus.OperationIdentity{}, err
	}
	return commonnexus.OperationIdentity{
		NamespaceID:      componentRef.GetNamespaceId(),
		WorkflowID:       componentRef.GetBusinessId(),
		RunID:            componentRef.GetRunId(),
		ScheduledEventID: scheduledEventID,
		RequestID:        completion.GetRequestId(),
	}, nil
}

// CompletionFromOperationIdentity builds a CHASM-format Nexus completion token addressing the operation identity.
// The ComponentRef carries no versioned transitions, so it MUST be resolved at
// chasm.RefConsistencyLevelComponentCreation or chasm.RefConsistencyLevelCurrentRun. It must NOT be resolved at
// the default chasm.RefConsistencyLevelExecutionLastUpdate: with a nil execution transition the staleness check is
// silently skipped (chasm.Node.IsStale returns nil) rather than failing.
func CompletionFromOperationIdentity(id commonnexus.OperationIdentity) (*tokenspb.NexusOperationCompletion, error) {
	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId:   id.NamespaceID,
		BusinessId:    id.WorkflowID,
		RunId:         id.RunID,
		ArchetypeId:   chasm.WorkflowArchetypeID,
		ComponentPath: []string{operationsFieldName, strconv.FormatInt(id.ScheduledEventID, 10)},
	}).Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal component ref: %w", err)
	}
	return &tokenspb.NexusOperationCompletion{
		ComponentRef: componentRef,
		RequestId:    id.RequestID,
	}, nil
}

// scheduledEventIDFromComponentPath extracts the scheduled event ID from a Nexus operation
// component path of the form ["Operations", "<scheduledEventID>"].
func scheduledEventIDFromComponentPath(path []string) (int64, error) {
	if len(path) != 2 || path[0] != operationsFieldName {
		return 0, fmt.Errorf("unexpected nexus operation component path %v", path)
	}
	scheduledEventID, err := strconv.ParseInt(path[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid scheduled event id %q: %w", path[1], err)
	}
	return scheduledEventID, nil
}
