package cassandra

import (
	"context"
	"fmt"
	"sync"

	cgocql "github.com/gocql/gocql"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const DefaultTaskQueueUserDataBucketCount = 64

// TaskQueueUserDataMigrationMode is the Cassandra configuration type used by this store.
type TaskQueueUserDataMigrationMode = config.CassandraTaskQueueUserDataMigrationMode

const (
	TaskQueueUserDataMigrationModeSourceOnly   = config.CassandraTaskQueueUserDataMigrationModeSourceOnly
	TaskQueueUserDataMigrationModeSourceDual   = config.CassandraTaskQueueUserDataMigrationModeSourceDual
	TaskQueueUserDataMigrationModeTargetShadow = config.CassandraTaskQueueUserDataMigrationModeTargetShadow
	TaskQueueUserDataMigrationModeTargetDual   = config.CassandraTaskQueueUserDataMigrationModeTargetDual
	TaskQueueUserDataMigrationModeTargetOnly   = config.CassandraTaskQueueUserDataMigrationModeTargetOnly
)

func normalizeTaskQueueUserDataMigrationMode(mode TaskQueueUserDataMigrationMode) TaskQueueUserDataMigrationMode {
	if mode == "" {
		return TaskQueueUserDataMigrationModeSourceOnly
	}
	return mode
}

func ValidateTaskQueueUserDataMigrationMode(mode TaskQueueUserDataMigrationMode) error {
	switch normalizeTaskQueueUserDataMigrationMode(mode) {
	case TaskQueueUserDataMigrationModeSourceOnly,
		TaskQueueUserDataMigrationModeSourceDual,
		TaskQueueUserDataMigrationModeTargetShadow,
		TaskQueueUserDataMigrationModeTargetDual,
		TaskQueueUserDataMigrationModeTargetOnly:
		return nil
	default:
		return fmt.Errorf("unsupported Cassandra task queue user data migration mode %q", mode)
	}
}

func newUserDataStore(
	session gocql.Session,
	logger log.Logger,
	mode TaskQueueUserDataMigrationMode,
	bucketCount int,
	generation ...cgocql.UUID,
) userDataStore {
	if bucketCount <= 0 {
		bucketCount = DefaultTaskQueueUserDataBucketCount
	}
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	identity := &taskQueueUserDataAuthorityIdentityCache{}
	if len(generation) > 0 && generation[0] != (cgocql.UUID{}) {
		identity.loaded = true
		identity.generation = generation[0]
	}
	return userDataStore{
		Session:              session,
		logger:               logger,
		migrationMode:        normalizeTaskQueueUserDataMigrationMode(mode),
		bucketCount:          bucketCount,
		authorityIdentity:    identity,
		targetAuthorityCache: &sync.Map{},
	}
}

func (d *userDataStore) GetTaskQueueUserData(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (*p.InternalGetTaskQueueUserDataResponse, error) {
	if normalizeTaskQueueUserDataMigrationMode(d.migrationMode) == TaskQueueUserDataMigrationModeSourceOnly {
		return d.getTaskQueueUserDataV1(ctx, request)
	}
	authority, err := d.resolveTaskQueueUserDataAuthority(ctx, request.NamespaceID)
	if err != nil {
		return nil, err
	}
	if authority == taskQueueUserDataAuthorityUnspecified || authority == taskQueueUserDataAuthoritySource {
		return d.getTaskQueueUserDataV1(ctx, request)
	}
	if authority == taskQueueUserDataAuthorityTarget {
		return d.getTaskQueueUserDataV2(ctx, request)
	}
	return nil, serviceerror.NewUnavailable("task queue user data namespace cutover is in progress")
}

func (d *userDataStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) error {
	if normalizeTaskQueueUserDataMigrationMode(d.migrationMode) == TaskQueueUserDataMigrationModeSourceOnly {
		return d.updateTaskQueueUserDataV1Guarded(
			ctx,
			request,
			taskQueueUserDataAuthorityRecord{},
			taskQueueUserDataAuthorityUnspecified,
		)
	}
	authority, err := d.resolveTaskQueueUserDataAuthority(ctx, request.NamespaceID)
	if err != nil {
		return err
	}
	identity := taskQueueUserDataAuthorityRecord{}
	if authority != taskQueueUserDataAuthorityUnspecified &&
		normalizeTaskQueueUserDataMigrationMode(d.migrationMode) != TaskQueueUserDataMigrationModeTargetOnly {
		identity, err = d.taskQueueUserDataAuthorityIdentity(ctx)
		if err != nil {
			return err
		}
	}
	switch normalizeTaskQueueUserDataMigrationMode(d.migrationMode) {
	case TaskQueueUserDataMigrationModeSourceDual,
		TaskQueueUserDataMigrationModeTargetShadow:
		if err := d.updateTaskQueueUserDataV1Guarded(ctx, request, identity, authority); err != nil {
			return err
		}
		d.mirrorTaskQueueUserDataUpdate(
			"Unable to mirror task queue user data update to v2",
			func() error { return d.updateTaskQueueUserDataV2(ctx, taskQueueUserDataMirrorRequest(request)) },
		)
		return nil
	case TaskQueueUserDataMigrationModeTargetDual:
		if authority == taskQueueUserDataAuthoritySource {
			if err := d.updateTaskQueueUserDataV1Guarded(ctx, request, identity, authority); err != nil {
				return err
			}
			d.mirrorTaskQueueUserDataUpdate(
				"Unable to mirror task queue user data update to v2",
				func() error { return d.updateTaskQueueUserDataV2(ctx, taskQueueUserDataMirrorRequest(request)) },
			)
			return nil
		}
		if err := d.updateTaskQueueUserDataV2(ctx, request); err != nil {
			return err
		}
		d.mirrorTaskQueueUserDataUpdate(
			"Unable to mirror task queue user data update to v1",
			func() error {
				return d.updateTaskQueueUserDataV1Guarded(
					ctx,
					taskQueueUserDataMirrorRequest(request),
					identity,
					taskQueueUserDataAuthorityTarget,
				)
			},
		)
		return nil
	case TaskQueueUserDataMigrationModeTargetOnly:
		return d.updateTaskQueueUserDataV2(ctx, request)
	default:
		return unsupportedTaskQueueUserDataMigrationMode(d.migrationMode)
	}
}

func (d *userDataStore) mirrorTaskQueueUserDataUpdate(
	message string,
	update func() error,
) {
	if err := update(); err != nil {
		d.logger.Error(message, tag.Error(err))
	}
}

func (d *userDataStore) ListTaskQueueUserDataEntries(
	ctx context.Context,
	request *p.ListTaskQueueUserDataEntriesRequest,
) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	if normalizeTaskQueueUserDataMigrationMode(d.migrationMode) == TaskQueueUserDataMigrationModeSourceOnly {
		return d.listTaskQueueUserDataEntriesV1(ctx, request)
	}
	authority, err := d.resolveTaskQueueUserDataAuthority(ctx, request.NamespaceID)
	if err != nil {
		return nil, err
	}
	if authority == taskQueueUserDataAuthorityUnspecified || authority == taskQueueUserDataAuthoritySource {
		return d.listTaskQueueUserDataEntriesV1(ctx, request)
	}
	if authority == taskQueueUserDataAuthorityTarget {
		return d.listTaskQueueUserDataEntriesV2(ctx, request)
	}
	return nil, serviceerror.NewUnavailable("task queue user data namespace cutover is in progress")
}

//nolint:staticcheck // The persistence interface retains BuildId for API compatibility.
func (d *userDataStore) GetTaskQueuesByBuildId(
	ctx context.Context,
	request *p.GetTaskQueuesByBuildIdRequest,
) ([]string, error) {
	if normalizeTaskQueueUserDataMigrationMode(d.migrationMode) == TaskQueueUserDataMigrationModeSourceOnly {
		return d.getTaskQueuesByBuildIDV1(ctx, request)
	}
	authority, err := d.resolveTaskQueueUserDataAuthority(ctx, request.NamespaceID)
	if err != nil {
		return nil, err
	}
	if authority == taskQueueUserDataAuthorityUnspecified || authority == taskQueueUserDataAuthoritySource {
		return d.getTaskQueuesByBuildIDV1(ctx, request)
	}
	if authority == taskQueueUserDataAuthorityTarget {
		return d.getTaskQueuesByBuildIDV2(ctx, request)
	}
	return nil, serviceerror.NewUnavailable("task queue user data namespace cutover is in progress")
}

//nolint:staticcheck // The persistence interface retains BuildId for API compatibility.
func (d *userDataStore) CountTaskQueuesByBuildId(
	ctx context.Context,
	request *p.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	if normalizeTaskQueueUserDataMigrationMode(d.migrationMode) == TaskQueueUserDataMigrationModeSourceOnly {
		return d.countTaskQueuesByBuildIDV1(ctx, request)
	}
	authority, err := d.resolveTaskQueueUserDataAuthority(ctx, request.NamespaceID)
	if err != nil {
		return 0, err
	}
	if authority == taskQueueUserDataAuthorityUnspecified || authority == taskQueueUserDataAuthoritySource {
		return d.countTaskQueuesByBuildIDV1(ctx, request)
	}
	if authority == taskQueueUserDataAuthorityTarget {
		return d.countTaskQueuesByBuildIDV2(ctx, request)
	}
	return 0, serviceerror.NewUnavailable("task queue user data namespace cutover is in progress")
}

func taskQueueUserDataMirrorRequest(request *p.InternalUpdateTaskQueueUserDataRequest) *p.InternalUpdateTaskQueueUserDataRequest {
	updates := make(map[string]*p.InternalSingleTaskQueueUserDataUpdate, len(request.Updates))
	for taskQueue, update := range request.Updates {
		copyUpdate := *update
		copyUpdate.Applied = nil
		copyUpdate.Conflicting = nil
		updates[taskQueue] = &copyUpdate
	}
	return &p.InternalUpdateTaskQueueUserDataRequest{
		NamespaceID: request.NamespaceID,
		Updates:     updates,
	}
}

func unsupportedTaskQueueUserDataMigrationMode(mode TaskQueueUserDataMigrationMode) error {
	return fmt.Errorf("unsupported Cassandra task queue user data migration mode %q", mode)
}
