package nexus

// OperationIdentity is the framework-agnostic identity of a Nexus operation: the fields shared by
// the HSM and CHASM completion-token representations that are needed to locate and validate an
// operation regardless of which framework currently backs it.
//
// It is the interchange type for cross-framework completion-token conversion. The framework that
// owns each representation provides the extract/build helpers (see components/nexusoperations for
// HSM and chasm/lib/workflow for CHASM), so callers can convert a token between frameworks without
// knowing either framework's ref layout.
type OperationIdentity struct {
	NamespaceID      string
	WorkflowID       string
	RunID            string
	ScheduledEventID int64
	RequestID        string
}
