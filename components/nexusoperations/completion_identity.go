package nexusoperations

import (
	"errors"
	"fmt"
	"strconv"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	commonnexus "go.temporal.io/server/common/nexus"
)

// OperationIdentityFromCompletion extracts the framework-agnostic operation identity from an
// HSM-format Nexus completion token. The scheduled event ID is read from the operation state
// machine key in the ref path; the remaining fields are top-level on the token.
func OperationIdentityFromCompletion(completion *tokenspb.NexusOperationCompletion) (commonnexus.OperationIdentity, error) {
	scheduledEventID, err := scheduledEventIDFromRef(completion.GetRef())
	if err != nil {
		return commonnexus.OperationIdentity{}, err
	}
	return commonnexus.OperationIdentity{
		NamespaceID:      completion.GetNamespaceId(),
		WorkflowID:       completion.GetWorkflowId(),
		RunID:            completion.GetRunId(),
		ScheduledEventID: scheduledEventID,
		RequestID:        completion.GetRequestId(),
	}, nil
}

// CompletionFromOperationIdentity builds an HSM-format Nexus completion token addressing the operation identity.
// Versioned-transition fields are set to non-nil zero values so the completion handler resolves the operation
// via its run fallback / non-transition-history validation (a zero transition count) rather than an exact-transition
// match; identity is re-established by request ID.
func CompletionFromOperationIdentity(id commonnexus.OperationIdentity) *tokenspb.NexusOperationCompletion {
	return &tokenspb.NexusOperationCompletion{
		NamespaceId: id.NamespaceID,
		WorkflowId:  id.WorkflowID,
		RunId:       id.RunID,
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{
				Type: OperationMachineType,
				Id:   strconv.FormatInt(id.ScheduledEventID, 10),
			}},
			MachineInitialVersionedTransition:    &persistencespb.VersionedTransition{},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{},
		},
		RequestId: id.RequestID,
	}
}

// scheduledEventIDFromRef extracts the scheduled event ID from an operation ref, whose path
// addresses the operation state machine by Type=OperationMachineType, ID=<scheduledEventID>.
func scheduledEventIDFromRef(ref *persistencespb.StateMachineRef) (int64, error) {
	for _, key := range ref.GetPath() {
		if key.GetType() != OperationMachineType {
			continue
		}
		scheduledEventID, err := strconv.ParseInt(key.GetId(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid operation state machine id %q: %w", key.GetId(), err)
		}
		return scheduledEventID, nil
	}
	return 0, errors.New("completion token has no operation state machine reference")
}
