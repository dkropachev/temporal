package cassandra

import (
	"context"
	"errors"
	"fmt"

	"github.com/gocql/gocql"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func SchemaLayoutSpecForTable(
	ctx context.Context,
	session commongocql.Session,
	keyspace string,
	name SchemaLayoutName,
	table string,
	immutableParameter int64,
) (SchemaLayoutSpec, error) {
	if keyspace == "" || table == "" {
		return SchemaLayoutSpec{}, fmt.Errorf("cassandra schema layout %q requires keyspace and target table", name)
	}
	var generation gocql.UUID
	if err := session.Query(
		templateGetHistoryNodeTableID,
		keyspace,
		table,
	).WithContext(ctx).Scan(&generation); err != nil {
		return SchemaLayoutSpec{}, fmt.Errorf("read target table generation for %s.%s: %w", keyspace, table, err)
	}
	return SchemaLayoutSpec{
		Name:               name,
		Generation:         generation,
		Version:            1,
		ImmutableParameter: immutableParameter,
	}, nil
}

func RequireSchemaLayoutTargetOnly(
	ctx context.Context,
	session commongocql.Session,
	keyspace string,
	name SchemaLayoutName,
	table string,
	immutableParameter int64,
) error {
	spec, err := SchemaLayoutSpecForTable(ctx, session, keyspace, name, table, immutableParameter)
	if err != nil {
		return err
	}
	if _, err := NewSchemaLayoutMetadataStore(session).RequireTargetOnly(ctx, spec); err != nil {
		return fmt.Errorf("require Cassandra schema layout %q target-only: %w", name, err)
	}
	return nil
}

func RequireSchemaLayoutTargetReady(
	ctx context.Context,
	session commongocql.Session,
	keyspace string,
	name SchemaLayoutName,
	table string,
	immutableParameter int64,
) error {
	spec, err := SchemaLayoutSpecForTable(ctx, session, keyspace, name, table, immutableParameter)
	if err != nil {
		return err
	}
	if _, err := NewSchemaLayoutMetadataStore(session).RequireTargetReady(ctx, spec); err != nil {
		return fmt.Errorf("require Cassandra schema layout %q target-ready: %w", name, err)
	}
	return nil
}

func RequireSchemaLayoutExactTargetReady(
	ctx context.Context,
	session commongocql.Session,
	keyspace string,
	name SchemaLayoutName,
	table string,
	immutableParameter int64,
) error {
	spec, err := SchemaLayoutSpecForTable(ctx, session, keyspace, name, table, immutableParameter)
	if err != nil {
		return err
	}
	if _, err := NewSchemaLayoutMetadataStore(session).RequireExactTargetReady(ctx, spec); err != nil {
		return fmt.Errorf("require Cassandra schema layout %q exact target-ready: %w", name, err)
	}
	return nil
}

func RequireSchemaLayoutSourceCompatible(
	ctx context.Context,
	session commongocql.Session,
	keyspace string,
	name SchemaLayoutName,
	table string,
	immutableParameter int64,
) error {
	var metadataTableGeneration gocql.UUID
	err := session.Query(
		templateGetHistoryNodeTableID,
		keyspace,
		"schema_layout_metadata",
	).WithContext(ctx).Scan(&metadataTableGeneration)
	if commongocql.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Cassandra schema layout metadata table generation: %w", err)
	}
	metadataStore := NewSchemaLayoutMetadataStore(session)
	metadata, err := metadataStore.Load(ctx, name)
	var notFound *SchemaLayoutNotFoundError
	if errors.As(err, &notFound) {
		return nil
	}
	if err != nil {
		return err
	}
	spec, err := SchemaLayoutSpecForTable(ctx, session, keyspace, name, table, immutableParameter)
	if err != nil {
		return err
	}
	if metadata.SchemaLayoutSpec != spec {
		return &SchemaLayoutMismatchError{Expected: spec, Actual: metadata.SchemaLayoutSpec}
	}
	switch metadata.AuthorityState {
	case SchemaLayoutAuthorityPreparing, SchemaLayoutAuthorityTargetReady:
		return nil
	case SchemaLayoutAuthorityTargetOnly, SchemaLayoutAuthorityRetired:
		return &SchemaLayoutAuthorityError{
			Name:     name,
			Expected: []SchemaLayoutAuthorityState{SchemaLayoutAuthorityPreparing, SchemaLayoutAuthorityTargetReady},
			Actual:   metadata.AuthorityState,
		}
	default:
		return fmt.Errorf("unsupported Cassandra schema layout authority %q", metadata.AuthorityState)
	}
}

func InitializeSchemaLayoutTargetOnly(
	ctx context.Context,
	session commongocql.Session,
	keyspace string,
	name SchemaLayoutName,
	table string,
	immutableParameter int64,
) error {
	spec, err := SchemaLayoutSpecForTable(ctx, session, keyspace, name, table, immutableParameter)
	if err != nil {
		return err
	}
	store := NewSchemaLayoutMetadataStore(session)
	if _, err := store.InitializePreparing(ctx, spec); err != nil {
		return err
	}
	if _, err := store.MarkTargetReady(ctx, spec); err != nil {
		return err
	}
	if _, err := store.MarkTargetOnly(ctx, spec); err != nil {
		return err
	}
	return nil
}
