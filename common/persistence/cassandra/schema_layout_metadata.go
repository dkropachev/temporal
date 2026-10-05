package cassandra

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateInitializeSchemaLayoutMetadata = `INSERT INTO schema_layout_metadata ` +
		`(layout_name, generation, layout_version, immutable_parameter, authority_state, epoch, updated_at) ` +
		`VALUES (?, ?, ?, ?, ?, ?, toTimestamp(now())) IF NOT EXISTS`
	templateGetSchemaLayoutMetadata = `SELECT generation, layout_version, immutable_parameter, ` +
		`authority_state, epoch, updated_at FROM schema_layout_metadata WHERE layout_name = ?`
	templateUpdateSchemaLayoutAuthority = `UPDATE schema_layout_metadata ` +
		`SET authority_state = ?, epoch = ?, updated_at = toTimestamp(now()) WHERE layout_name = ? ` +
		`IF generation = ? AND layout_version = ? AND immutable_parameter = ? ` +
		`AND authority_state = ? AND epoch = ?`
)

// SchemaLayoutName identifies one independently migrated Cassandra table family.
type SchemaLayoutName string

const (
	SchemaLayoutExecutions        SchemaLayoutName = "executions"
	SchemaLayoutHistoryNode       SchemaLayoutName = "history_node"
	SchemaLayoutHistoryTree       SchemaLayoutName = "history_tree"
	SchemaLayoutQueueV2Metadata   SchemaLayoutName = "queue_v2_metadata"
	SchemaLayoutQueueV2Messages   SchemaLayoutName = "queue_v2_messages"
	SchemaLayoutLegacyQueue       SchemaLayoutName = "legacy_queue"
	SchemaLayoutMatchingTasks     SchemaLayoutName = "matching_tasks"
	SchemaLayoutMatchingTasksFair SchemaLayoutName = "matching_tasks_fair"
	SchemaLayoutTaskQueueUserData SchemaLayoutName = "task_queue_user_data"
)

var schemaLayoutNames = [...]SchemaLayoutName{
	SchemaLayoutExecutions,
	SchemaLayoutHistoryNode,
	SchemaLayoutHistoryTree,
	SchemaLayoutQueueV2Metadata,
	SchemaLayoutQueueV2Messages,
	SchemaLayoutLegacyQueue,
	SchemaLayoutMatchingTasks,
	SchemaLayoutMatchingTasksFair,
	SchemaLayoutTaskQueueUserData,
}

// SchemaLayoutAuthorityState records which layout can serve authoritative traffic.
type SchemaLayoutAuthorityState string

const (
	SchemaLayoutAuthorityPreparing   SchemaLayoutAuthorityState = "preparing"
	SchemaLayoutAuthorityTargetReady SchemaLayoutAuthorityState = "target-ready"
	SchemaLayoutAuthorityTargetOnly  SchemaLayoutAuthorityState = "target-only"
	SchemaLayoutAuthorityRetired     SchemaLayoutAuthorityState = "retired"
)

// SchemaLayoutSpec contains identity that cannot change during one layout generation.
type SchemaLayoutSpec struct {
	Name               SchemaLayoutName
	Generation         gocql.UUID
	Version            int32
	ImmutableParameter int64
}

// SchemaLayoutMetadata is the persisted authority record for one layout family.
type SchemaLayoutMetadata struct {
	SchemaLayoutSpec
	AuthorityState SchemaLayoutAuthorityState
	Epoch          int64
	UpdatedAt      time.Time
}

// SchemaLayoutMetadataStore persists schema migration authority independently from process config.
type SchemaLayoutMetadataStore struct {
	session commongocql.Session
}

// SchemaLayoutNotFoundError means no authority record exists for a required layout.
type SchemaLayoutNotFoundError struct {
	Name SchemaLayoutName
}

func (e *SchemaLayoutNotFoundError) Error() string {
	return fmt.Sprintf("Cassandra schema layout metadata %q does not exist", e.Name)
}

// SchemaLayoutMismatchError means persisted immutable identity differs from process expectations.
type SchemaLayoutMismatchError struct {
	Expected SchemaLayoutSpec
	Actual   SchemaLayoutSpec
}

func (e *SchemaLayoutMismatchError) Error() string {
	return fmt.Sprintf(
		"Cassandra schema layout metadata %q mismatch: expected generation %s, version %d, parameter %d; got generation %s, version %d, parameter %d",
		e.Expected.Name,
		e.Expected.Generation,
		e.Expected.Version,
		e.Expected.ImmutableParameter,
		e.Actual.Generation,
		e.Actual.Version,
		e.Actual.ImmutableParameter,
	)
}

// SchemaLayoutAuthorityError means a layout cannot serve the requested migration mode.
type SchemaLayoutAuthorityError struct {
	Name     SchemaLayoutName
	Expected []SchemaLayoutAuthorityState
	Actual   SchemaLayoutAuthorityState
}

func (e *SchemaLayoutAuthorityError) Error() string {
	return fmt.Sprintf(
		"Cassandra schema layout metadata %q has authority state %q; expected one of %v",
		e.Name,
		e.Actual,
		e.Expected,
	)
}

// SchemaLayoutConflictError means another process changed migration authority first.
type SchemaLayoutConflictError struct {
	Name          SchemaLayoutName
	ExpectedState SchemaLayoutAuthorityState
	ExpectedEpoch int64
	ActualState   SchemaLayoutAuthorityState
	ActualEpoch   int64
}

func (e *SchemaLayoutConflictError) Error() string {
	return fmt.Sprintf(
		"Cassandra schema layout metadata %q changed concurrently: expected state %q at epoch %d; got state %q at epoch %d",
		e.Name,
		e.ExpectedState,
		e.ExpectedEpoch,
		e.ActualState,
		e.ActualEpoch,
	)
}

// NewSchemaLayoutMetadataStore creates a schema layout authority store.
func NewSchemaLayoutMetadataStore(session commongocql.Session) *SchemaLayoutMetadataStore {
	return &SchemaLayoutMetadataStore{session: session}
}

// SchemaLayoutNames returns every layout family with persisted migration authority.
func SchemaLayoutNames() []SchemaLayoutName {
	names := make([]SchemaLayoutName, len(schemaLayoutNames))
	copy(names, schemaLayoutNames[:])
	return names
}

// InitializePreparing creates a preparing authority record or validates an existing record.
func (s *SchemaLayoutMetadataStore) InitializePreparing(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	if err := validateSchemaLayoutSpec(spec); err != nil {
		return SchemaLayoutMetadata{}, err
	}

	applied, err := s.session.Query(
		templateInitializeSchemaLayoutMetadata,
		spec.Name,
		spec.Generation,
		spec.Version,
		spec.ImmutableParameter,
		SchemaLayoutAuthorityPreparing,
		int64(0),
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"initialize Cassandra schema layout metadata %q: %w",
			spec.Name,
			commongocql.ConvertError("InitializeSchemaLayoutMetadata", err),
		)
	}
	if !applied {
		return s.LoadAndValidate(ctx, spec)
	}
	return s.LoadAndValidate(ctx, spec)
}

// Load reads one persisted authority record.
func (s *SchemaLayoutMetadataStore) Load(
	ctx context.Context,
	name SchemaLayoutName,
) (SchemaLayoutMetadata, error) {
	if err := validateSchemaLayoutName(name); err != nil {
		return SchemaLayoutMetadata{}, err
	}

	metadata := SchemaLayoutMetadata{SchemaLayoutSpec: SchemaLayoutSpec{Name: name}}
	err := s.session.Query(templateGetSchemaLayoutMetadata, name).WithContext(ctx).Scan(
		&metadata.Generation,
		&metadata.Version,
		&metadata.ImmutableParameter,
		&metadata.AuthorityState,
		&metadata.Epoch,
		&metadata.UpdatedAt,
	)
	if commongocql.IsNotFoundError(err) {
		return SchemaLayoutMetadata{}, &SchemaLayoutNotFoundError{Name: name}
	}
	if err != nil {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"load cassandra schema layout metadata %q: %w",
			name,
			commongocql.ConvertError("LoadSchemaLayoutMetadata", err),
		)
	}
	if !isSchemaLayoutAuthorityState(metadata.AuthorityState) {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"cassandra schema layout metadata %q has unsupported authority state %q",
			name,
			metadata.AuthorityState,
		)
	}
	if metadata.Epoch < 0 {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"cassandra schema layout metadata %q has negative epoch %d",
			name,
			metadata.Epoch,
		)
	}
	return metadata, nil
}

// LoadAndValidate loads metadata and rejects any immutable identity mismatch.
func (s *SchemaLayoutMetadataStore) LoadAndValidate(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	if err := validateSchemaLayoutSpec(spec); err != nil {
		return SchemaLayoutMetadata{}, err
	}
	metadata, err := s.Load(ctx, spec.Name)
	if err != nil {
		return SchemaLayoutMetadata{}, err
	}
	if metadata.SchemaLayoutSpec != spec {
		return SchemaLayoutMetadata{}, &SchemaLayoutMismatchError{
			Expected: spec,
			Actual:   metadata.SchemaLayoutSpec,
		}
	}
	return metadata, nil
}

// CompareAndSwapAuthority performs one legal, monotonic authority transition.
func (s *SchemaLayoutMetadataStore) CompareAndSwapAuthority(
	ctx context.Context,
	expected SchemaLayoutMetadata,
	next SchemaLayoutAuthorityState,
) (SchemaLayoutMetadata, error) {
	if err := validateSchemaLayoutSpec(expected.SchemaLayoutSpec); err != nil {
		return SchemaLayoutMetadata{}, err
	}
	if expected.Epoch < 0 {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"cassandra schema layout metadata %q expected epoch must not be negative",
			expected.Name,
		)
	}
	if !isLegalSchemaLayoutTransition(expected.AuthorityState, next) {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"illegal Cassandra schema layout authority transition %q -> %q for %q",
			expected.AuthorityState,
			next,
			expected.Name,
		)
	}

	applied, err := s.session.Query(
		templateUpdateSchemaLayoutAuthority,
		next,
		expected.Epoch+1,
		expected.Name,
		expected.Generation,
		expected.Version,
		expected.ImmutableParameter,
		expected.AuthorityState,
		expected.Epoch,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return SchemaLayoutMetadata{}, fmt.Errorf(
			"transition Cassandra schema layout metadata %q to %q: %w",
			expected.Name,
			next,
			commongocql.ConvertError("CompareAndSwapSchemaLayoutAuthority", err),
		)
	}
	if !applied {
		actual, loadErr := s.Load(ctx, expected.Name)
		if loadErr != nil {
			return SchemaLayoutMetadata{}, loadErr
		}
		if actual.SchemaLayoutSpec != expected.SchemaLayoutSpec {
			return SchemaLayoutMetadata{}, &SchemaLayoutMismatchError{
				Expected: expected.SchemaLayoutSpec,
				Actual:   actual.SchemaLayoutSpec,
			}
		}
		return SchemaLayoutMetadata{}, &SchemaLayoutConflictError{
			Name:          expected.Name,
			ExpectedState: expected.AuthorityState,
			ExpectedEpoch: expected.Epoch,
			ActualState:   actual.AuthorityState,
			ActualEpoch:   actual.Epoch,
		}
	}
	return s.LoadAndValidate(ctx, expected.SchemaLayoutSpec)
}

// MarkTargetReady records that target data is complete and validated.
func (s *SchemaLayoutMetadataStore) MarkTargetReady(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	return s.markAuthority(ctx, spec, SchemaLayoutAuthorityTargetReady)
}

// MarkTargetOnly disables migration-path reads and writes for this layout family.
func (s *SchemaLayoutMetadataStore) MarkTargetOnly(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	return s.markAuthority(ctx, spec, SchemaLayoutAuthorityTargetOnly)
}

// MarkRetired permanently removes this layout generation from service.
func (s *SchemaLayoutMetadataStore) MarkRetired(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	return s.markAuthority(ctx, spec, SchemaLayoutAuthorityRetired)
}

// RequireTargetReady validates startup for modes that may use the target as authority.
func (s *SchemaLayoutMetadataStore) RequireTargetReady(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	return s.requireAuthority(
		ctx,
		spec,
		SchemaLayoutAuthorityTargetReady,
		SchemaLayoutAuthorityTargetOnly,
	)
}

// RequireExactTargetReady validates startup for dual modes that must stop after terminal cutover.
func (s *SchemaLayoutMetadataStore) RequireExactTargetReady(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	return s.requireAuthority(ctx, spec, SchemaLayoutAuthorityTargetReady)
}

// RequireTargetOnly validates startup for fully migrated, non-dual operation.
func (s *SchemaLayoutMetadataStore) RequireTargetOnly(
	ctx context.Context,
	spec SchemaLayoutSpec,
) (SchemaLayoutMetadata, error) {
	return s.requireAuthority(ctx, spec, SchemaLayoutAuthorityTargetOnly)
}

func (s *SchemaLayoutMetadataStore) markAuthority(
	ctx context.Context,
	spec SchemaLayoutSpec,
	target SchemaLayoutAuthorityState,
) (SchemaLayoutMetadata, error) {
	for {
		metadata, err := s.LoadAndValidate(ctx, spec)
		if err != nil {
			return SchemaLayoutMetadata{}, err
		}
		if metadata.AuthorityState == target {
			return metadata, nil
		}
		if target == SchemaLayoutAuthorityTargetReady && metadata.AuthorityState == SchemaLayoutAuthorityTargetOnly {
			return metadata, nil
		}
		if !isLegalSchemaLayoutTransition(metadata.AuthorityState, target) {
			return SchemaLayoutMetadata{}, newSchemaLayoutAuthorityError(metadata, target)
		}

		updated, err := s.CompareAndSwapAuthority(ctx, metadata, target)
		var conflict *SchemaLayoutConflictError
		if errors.As(err, &conflict) {
			continue
		}
		return updated, err
	}
}

func (s *SchemaLayoutMetadataStore) requireAuthority(
	ctx context.Context,
	spec SchemaLayoutSpec,
	allowed ...SchemaLayoutAuthorityState,
) (SchemaLayoutMetadata, error) {
	metadata, err := s.LoadAndValidate(ctx, spec)
	if err != nil {
		return SchemaLayoutMetadata{}, err
	}
	for _, state := range allowed {
		if metadata.AuthorityState == state {
			return metadata, nil
		}
	}
	return SchemaLayoutMetadata{}, &SchemaLayoutAuthorityError{
		Name:     spec.Name,
		Expected: allowed,
		Actual:   metadata.AuthorityState,
	}
}

func newSchemaLayoutAuthorityError(
	metadata SchemaLayoutMetadata,
	expected ...SchemaLayoutAuthorityState,
) error {
	return &SchemaLayoutAuthorityError{
		Name:     metadata.Name,
		Expected: expected,
		Actual:   metadata.AuthorityState,
	}
}

func validateSchemaLayoutSpec(spec SchemaLayoutSpec) error {
	if err := validateSchemaLayoutName(spec.Name); err != nil {
		return err
	}
	if spec.Generation == (gocql.UUID{}) {
		return fmt.Errorf("cassandra schema layout metadata %q generation must not be empty", spec.Name)
	}
	if spec.Version <= 0 {
		return fmt.Errorf("cassandra schema layout metadata %q version must be positive", spec.Name)
	}
	if spec.ImmutableParameter < 0 {
		return fmt.Errorf("cassandra schema layout metadata %q immutable parameter must not be negative", spec.Name)
	}
	return nil
}

func validateSchemaLayoutName(name SchemaLayoutName) error {
	for _, supported := range schemaLayoutNames {
		if name == supported {
			return nil
		}
	}
	return fmt.Errorf("unsupported Cassandra schema layout name %q", name)
}

func isSchemaLayoutAuthorityState(state SchemaLayoutAuthorityState) bool {
	switch state {
	case SchemaLayoutAuthorityPreparing,
		SchemaLayoutAuthorityTargetReady,
		SchemaLayoutAuthorityTargetOnly,
		SchemaLayoutAuthorityRetired:
		return true
	default:
		return false
	}
}

func isLegalSchemaLayoutTransition(
	from SchemaLayoutAuthorityState,
	to SchemaLayoutAuthorityState,
) bool {
	switch from {
	case SchemaLayoutAuthorityPreparing:
		return to == SchemaLayoutAuthorityTargetReady
	case SchemaLayoutAuthorityTargetReady:
		return to == SchemaLayoutAuthorityTargetOnly
	case SchemaLayoutAuthorityTargetOnly:
		return to == SchemaLayoutAuthorityRetired
	default:
		return false
	}
}
