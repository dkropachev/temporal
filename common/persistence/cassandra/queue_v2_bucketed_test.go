package cassandra

import (
	"sort"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func TestQueueV2MigrationModeLayouts(t *testing.T) {
	tests := []struct {
		name      string
		mode      config.CassandraQueueV2MigrationMode
		read      queueV2MetadataLayout
		write     queueV2MetadataLayout
		mirror    queueV2MetadataLayout
		hasMirror bool
	}{
		{
			name:  "source only",
			mode:  config.CassandraQueueV2MigrationModeSourceOnly,
			read:  queueV2MetadataLayoutSource,
			write: queueV2MetadataLayoutSource,
		},
		{
			name:      "source dual",
			mode:      config.CassandraQueueV2MigrationModeSourceDual,
			read:      queueV2MetadataLayoutSource,
			write:     queueV2MetadataLayoutSource,
			mirror:    queueV2MetadataLayoutTarget,
			hasMirror: true,
		},
		{
			name:      "target shadow",
			mode:      config.CassandraQueueV2MigrationModeTargetShadow,
			read:      queueV2MetadataLayoutSource,
			write:     queueV2MetadataLayoutSource,
			mirror:    queueV2MetadataLayoutTarget,
			hasMirror: true,
		},
		{
			name:      "target dual",
			mode:      config.CassandraQueueV2MigrationModeTargetDual,
			read:      queueV2MetadataLayoutTarget,
			write:     queueV2MetadataLayoutTarget,
			mirror:    queueV2MetadataLayoutSource,
			hasMirror: true,
		},
		{
			name:  "target only",
			mode:  config.CassandraQueueV2MigrationModeTargetOnly,
			read:  queueV2MetadataLayoutTarget,
			write: queueV2MetadataLayoutTarget,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &queueV2Store{migrationMode: test.mode}
			require.Equal(t, test.read, store.metadataReadLayout())
			write, mirror, hasMirror := store.metadataWriteLayouts()
			require.Equal(t, test.write, write)
			require.Equal(t, test.mirror, mirror)
			require.Equal(t, test.hasMirror, hasMirror)
		})
	}
}

func TestValidateQueueV2MigrationMode(t *testing.T) {
	require.NoError(t, ValidateQueueV2MigrationMode(""))
	require.NoError(t, ValidateQueueV2MigrationMode(config.CassandraQueueV2MigrationModeTargetOnly))
	require.Error(t, ValidateQueueV2MigrationMode("invalid"))
}

func TestQueueV2MetadataBucketIsStable(t *testing.T) {
	first := queueV2MetadataBucket(p.QueueTypeHistoryNormal, "test-queue")
	require.GreaterOrEqual(t, first, 0)
	require.Less(t, first, queueV2MetadataBucketCount)
	require.Equal(t, first, queueV2MetadataBucket(p.QueueTypeHistoryNormal, "test-queue"))
	require.NotEqual(t, first, queueV2MetadataBucket(p.QueueTypeHistoryDLQ, "test-queue"))
}

func TestQueueV2TargetOnlyCreateUsesOnlyBucketedMetadata(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		},
	}
	store, err := newQueueV2StoreWithMigrationIdentity(
		session,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeTargetOnly,
		DefaultQueueV2MessageBucketSpan,
		gocql.TimeUUID(),
		true,
	)
	require.NoError(t, err)

	_, err = store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: "test-queue",
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		templateCreateQueueV2TargetWithAuthority,
		templateInsertQueueV2MessageDirectory,
		templateInsertQueueV2MessageBucketState,
		templateTransitionQueueV2TargetMetadataAuthority,
	}, recordedStatements(session.queries))
	require.Equal(
		t,
		queueV2MetadataBucket(p.QueueTypeHistoryDLQ, "test-queue"),
		session.queries[0].args[1],
	)
}

func TestQueueV2SourceDualCreateWritesBothLayouts(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, _ ...any) cgocql.Query {
			if stmt == TemplateGetQueueV2Query || stmt == templateGetQueueV2SourceMessageAuthority {
				return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
			}
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		},
	}
	store, err := NewQueueV2StoreWithMigrationMode(
		session,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeSourceDual,
	)
	require.NoError(t, err)

	_, err = store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		QueueName: "test-queue",
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		templateCreateQueueV2SourceWithAuthority,
		templateGetQueueV2SourceMessageAuthority,
		templateInitializeQueueV2SourceMessageAuthority,
		TemplateGetQueueV2Query,
		templateCreateQueueV2TargetWithAuthority,
		templateInsertQueueV2MessageDirectory,
		templateInsertQueueV2MessageBucketState,
	}, recordedStatements(session.queries))
}

func TestQueueV2TargetOnlyReadUsesBucketedMetadata(t *testing.T) {
	queueBytes := marshalQueueV2Metadata(t, p.FirstQueueMessageID)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueV2Query:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*[]byte) = queueBytes
				*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
				*dest[2].(*int64) = 0
				return nil
			}}
		case templateGetQueueV2MessageDirectory:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 0
				*dest[1].(*int64) = DefaultQueueV2MessageBucketSpan
				*dest[2].(*int64) = 0
				return nil
			}}
		case templateGetQueueV2MessageBucketState:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 0
				*dest[1].(*int64) = 1
				return nil
			}}
		case templateGetQueueV2MessagesV3:
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
				{int64(0), []byte("message"), enumspb.ENCODING_TYPE_PROTO3.String()},
			}}}
		default:
			return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
		}
	}
	store, err := NewQueueV2StoreWithMigrationMode(
		session,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeTargetOnly,
	)
	require.NoError(t, err)
	store.(*queueV2Store).cacheQueueV2Authority(
		p.QueueTypeHistoryNormal,
		"test-queue",
		queueV2TestTargetAuthority(DefaultQueueV2MessageBucketSpan),
	)

	response, err := store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
		PageSize:  10,
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateGetQueueV2Query,
		templateGetQueueV2MessageDirectory,
		templateGetQueueV2MessageBucketState,
		templateGetQueueV2MessagesV3,
	}, recordedStatements(session.queries))
	require.Len(t, response.Messages, 1)
}

func TestQueueV2TargetOnlyEnqueueUsesBucketedMetadata(t *testing.T) {
	store := &queueV2Store{migrationMode: config.CassandraQueueV2MigrationModeTargetOnly}
	require.Equal(t, queueV2MessageLayoutTarget, store.messageReadLayout())
	primary, _, hasMirror := store.messageWriteLayouts()
	require.Equal(t, queueV2MessageLayoutTarget, primary)
	require.False(t, hasMirror)
	require.Contains(t, templateInsertQueueV2MessageV3, "queue_messages_v3")
	require.NotContains(t, templateInsertQueueV2MessageV3, "queue_messages (")
	require.Contains(t, templateAdvanceQueueV2MessageBucketTail, "IF last_message_id")
}

func TestQueueV2TargetOnlyUpdateUsesBucketedMetadata(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{mapScanCASFn: func(map[string]any) (bool, error) {
				return true, nil
			}}
		},
	}
	store := &queueV2Store{
		session:       session,
		logger:        log.NewNoopLogger(),
		migrationMode: config.CassandraQueueV2MigrationModeTargetOnly,
	}
	queue := &Queue{
		Metadata: &persistencespb.Queue{Partitions: map[int32]*persistencespb.QueuePartition{
			0: {MinMessageId: 3},
		}},
		Version: 7,
	}

	err := store.updateQueue(
		t.Context(),
		queue,
		p.QueueTypeHistoryDLQ,
		"test-queue",
		queueV2OperationRoute{layout: queueV2MetadataLayoutTarget},
	)

	require.NoError(t, err)
	require.Equal(t, int64(8), queue.Version)
	require.Equal(t, []string{TemplateUpdateQueueMetadataV2Query}, recordedStatements(session.queries))
}

func TestQueueV2TargetDualUpdateMirrorsSourceMetadata(t *testing.T) {
	const queueName = "test-queue"
	record := queueV2TestTargetAuthority(DefaultQueueV2MessageBucketSpan)
	sourceBytes := marshalQueueV2Metadata(t, 2)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, _ ...any) cgocql.Query {
		switch stmt {
		case templateUpdateQueueV2TargetWithAuthority, templateUpdateQueueV2SourceWithAuthority:
			return &recordingQuery{mapScanCASFn: func(map[string]any) (bool, error) {
				return true, nil
			}}
		case TemplateGetQueueQuery:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*[]byte) = sourceBytes
				*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
				*dest[2].(*int64) = 7
				return nil
			}}
		case templateGetQueueV2SourceMetadataAuthority:
			return &recordingQuery{scanFn: queueV2TestAuthorityScan(record)}
		default:
			t.Fatalf("unexpected query: %s", stmt)
			return nil
		}
	}
	store := &queueV2Store{
		session:       session,
		logger:        log.NewNoopLogger(),
		migrationMode: config.CassandraQueueV2MigrationModeTargetDual,
		messageSpan:   DefaultQueueV2MessageBucketSpan,
	}
	queue := &Queue{
		Metadata: &persistencespb.Queue{Partitions: map[int32]*persistencespb.QueuePartition{
			0: {MinMessageId: 3},
		}},
		Version: 7,
	}

	err := store.updateQueue(
		t.Context(),
		queue,
		p.QueueTypeHistoryDLQ,
		queueName,
		queueV2OperationRoute{
			record:  record,
			layout:  queueV2MetadataLayoutTarget,
			mirror:  true,
			guarded: true,
		},
	)

	require.NoError(t, err)
	require.Equal(t, int64(8), queue.Version)
	require.Equal(t, []string{
		templateUpdateQueueV2TargetWithAuthority,
		TemplateGetQueueQuery,
		templateGetQueueV2SourceMetadataAuthority,
		templateUpdateQueueV2SourceWithAuthority,
	}, recordedStatements(session.queries))
}

func TestResolveQueueV2TargetDualMirrorsWithAllAuthoritiesFenced(t *testing.T) {
	const queueName = "test-queue"
	record := queueV2TestTargetAuthority(DefaultQueueV2MessageBucketSpan)
	queueBytes := marshalQueueV2Metadata(t, p.FirstQueueMessageID)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, _ ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*[]byte) = queueBytes
				*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
				*dest[2].(*int64) = 0
				return nil
			}}
		case templateGetQueueV2SourceMetadataAuthority,
			templateGetQueueV2TargetMetadataAuthority,
			templateGetQueueV2SourceMessageAuthority,
			templateGetQueueV2TargetMessageAuthority:
			return &recordingQuery{scanFn: queueV2TestAuthorityScan(record)}
		default:
			t.Fatalf("unexpected query: %s", stmt)
			return nil
		}
	}
	store := &queueV2Store{
		session:       session,
		migrationMode: config.CassandraQueueV2MigrationModeTargetDual,
		messageSpan:   DefaultQueueV2MessageBucketSpan,
	}

	route, err := store.resolveQueueV2Route(t.Context(), p.QueueTypeHistoryDLQ, queueName)

	require.NoError(t, err)
	require.Equal(t, record, route.record)
	require.Equal(t, queueV2MetadataLayoutTarget, route.layout)
	require.True(t, route.mirror)
	require.True(t, route.guarded)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		templateGetQueueV2SourceMetadataAuthority,
		templateGetQueueV2TargetMetadataAuthority,
		templateGetQueueV2SourceMessageAuthority,
		templateGetQueueV2TargetMessageAuthority,
	}, recordedStatements(session.queries))
}

func queueV2TestAuthorityScan(record queueV2AuthorityRecord) func(...any) error {
	return func(dest ...any) error {
		authority := int(record.authority)
		generation := record.generation
		epoch := record.epoch
		span := record.messageSpan
		*dest[0].(**int) = &authority
		*dest[1].(**gocql.UUID) = &generation
		*dest[2].(**int64) = &epoch
		*dest[3].(**int64) = &span
		return nil
	}
}

func TestQueueV2TargetOnlyListUsesBucketQueriesAndPreservesCount(t *testing.T) {
	const queueName = "test-queue"
	queueBytes := marshalQueueV2Metadata(t, 3)
	bucket := queueV2MetadataBucket(p.QueueTypeHistoryDLQ, queueName)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case templateGetQueueNamesV2FirstPageQuery:
			if args[1] == bucket {
				return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
					{queueName, queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(4)},
				}}}
			}
			return &recordingQuery{iter: &recordingIter{}}
		case templateGetQueueV2MessageDirectory:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 0
				*dest[1].(*int64) = DefaultQueueV2MessageBucketSpan
				*dest[2].(*int64) = 0
				return nil
			}}
		case templateGetQueueV2MessageBucketState:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 7
				*dest[1].(*int64) = 8
				return nil
			}}
		default:
			return &recordingQuery{}
		}
	}
	store, err := NewQueueV2StoreWithMigrationMode(
		session,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeTargetOnly,
	)
	require.NoError(t, err)
	store.(*queueV2Store).cacheQueueV2Authority(
		p.QueueTypeHistoryDLQ,
		queueName,
		queueV2TestTargetAuthority(DefaultQueueV2MessageBucketSpan),
	)

	response, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  10,
	})

	require.NoError(t, err)
	require.Equal(t, []p.QueueInfo{{
		QueueName:     queueName,
		MessageCount:  5,
		LastMessageID: 7,
	}}, response.Queues)
	require.Empty(t, response.NextPageToken)
	statements := recordedStatements(session.queries)
	require.Len(t, statements, queueV2MetadataBucketCount+2)
	require.NotContains(t, statements, templateGetQueueNamesQuery)
	require.NotContains(t, statements, TemplateGetQueueQuery)
	require.NotContains(t, statements, TemplateGetMaxMessageIDQuery)
}

func TestQueueV2TargetOnlyListRejectsWrongMetadataBucket(t *testing.T) {
	const queueName = "wrong-bucket-queue"
	expectedBucket := queueV2MetadataBucket(p.QueueTypeHistoryDLQ, queueName)
	wrongBucket := (expectedBucket + 1) % queueV2MetadataBucketCount
	queueBytes := marshalQueueV2Metadata(t, p.FirstQueueMessageID)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		if stmt == templateGetQueueNamesV2FirstPageQuery && args[1] == wrongBucket {
			return &recordingQuery{iter: &recordingIter{scanRows: [][]any{
				{queueName, queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(1)},
			}}}
		}
		return &recordingQuery{iter: &recordingIter{}}
	}
	store, err := NewQueueV2StoreWithMigrationMode(
		session,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeTargetOnly,
	)
	require.NoError(t, err)
	_, err = store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  10,
	})

	require.ErrorContains(t, err, "expected")
}

func queueV2TestTargetAuthority(span int64) queueV2AuthorityRecord {
	return queueV2AuthorityRecord{
		authority:   queueV2MigrationAuthorityTarget,
		generation:  gocql.TimeUUID(),
		epoch:       int64(queueV2MigrationAuthorityTarget),
		messageSpan: span,
	}
}

func TestQueueV2TargetListPageToken(t *testing.T) {
	token := encodeQueueV2ListPageToken(p.QueueTypeHistoryDLQ, "queue-17")
	cursor, hasCursor, err := decodeQueueV2ListPageToken(token, p.QueueTypeHistoryDLQ)
	require.NoError(t, err)
	require.Equal(t, "queue-17", cursor)
	require.True(t, hasCursor)

	emptyCursor, hasEmptyCursor, err := decodeQueueV2ListPageToken(
		encodeQueueV2ListPageToken(p.QueueTypeHistoryDLQ, ""),
		p.QueueTypeHistoryDLQ,
	)
	require.NoError(t, err)
	require.Empty(t, emptyCursor)
	require.True(t, hasEmptyCursor)

	_, _, err = decodeQueueV2ListPageToken([]byte("legacy-page-state"), p.QueueTypeHistoryDLQ)
	require.ErrorIs(t, err, p.ErrInvalidListQueuesNextPageToken)

	badVersion := append([]byte(nil), token...)
	badVersion[len(queueV2ListTokenEnvelopePrefix)]++
	_, _, err = decodeQueueV2ListPageToken(badVersion, p.QueueTypeHistoryDLQ)
	require.ErrorIs(t, err, p.ErrInvalidListQueuesNextPageToken)

	_, _, err = decodeQueueV2ListPageToken(token, p.QueueTypeHistoryNormal)
	require.ErrorIs(t, err, p.ErrInvalidListQueuesNextPageToken)
}

func TestQueueV2TargetListPaginationMergesBuckets(t *testing.T) {
	names := []string{"zulu", "alpha", "hotel", "bravo", "yankee", "charlie", "delta"}
	sortedNames := append([]string(nil), names...)
	sort.Strings(sortedNames)
	queueBytes := marshalQueueV2Metadata(t, p.FirstQueueMessageID)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case templateGetQueueNamesV2FirstPageQuery, templateGetQueueNamesV2ContinuationQuery:
			bucket := args[1].(int)
			cursor := ""
			limitIndex := 2
			if stmt == templateGetQueueNamesV2ContinuationQuery {
				cursor = args[2].(string)
				limitIndex = 3
			}
			limit := args[limitIndex].(int)
			var rows [][]any
			for _, name := range sortedNames {
				if queueV2MetadataBucket(p.QueueTypeHistoryDLQ, name) != bucket || name <= cursor {
					continue
				}
				rows = append(rows, []any{
					name,
					queueBytes,
					enumspb.ENCODING_TYPE_PROTO3.String(),
					int64(0),
				})
				if len(rows) == limit {
					break
				}
			}
			return &recordingQuery{iter: &recordingIter{scanRows: rows}}
		case templateGetQueueV2MessageDirectory:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = 0
				*dest[1].(*int64) = DefaultQueueV2MessageBucketSpan
				*dest[2].(*int64) = 0
				return nil
			}}
		case templateGetQueueV2MessageBucketState:
			return &recordingQuery{scanFn: func(dest ...any) error {
				*dest[0].(*int64) = -1
				*dest[1].(*int64) = 0
				return nil
			}}
		default:
			return &recordingQuery{}
		}
	}
	store, err := NewQueueV2StoreWithMigrationMode(
		session,
		log.NewNoopLogger(),
		config.CassandraQueueV2MigrationModeTargetOnly,
	)
	require.NoError(t, err)
	for _, name := range names {
		store.(*queueV2Store).cacheQueueV2Authority(
			p.QueueTypeHistoryDLQ,
			name,
			queueV2TestTargetAuthority(DefaultQueueV2MessageBucketSpan),
		)
	}

	var (
		listed []string
		token  []byte
	)
	for {
		response, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
			QueueType:     p.QueueTypeHistoryDLQ,
			PageSize:      3,
			NextPageToken: token,
		})
		require.NoError(t, err)
		for _, queue := range response.Queues {
			listed = append(listed, queue.QueueName)
		}
		token = response.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	require.Equal(t, sortedNames, listed)
}

func TestBackfillQueueV2MetadataPage(t *testing.T) {
	queueBytes := marshalQueueV2Metadata(t, p.FirstQueueMessageID)
	session := &recordingSession{t: t}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case templateGetQueueNamesQuery:
			return &recordingQuery{iter: &recordingIter{
				pageState: []byte("next-page"),
				scanRows: [][]any{
					{"test-queue", queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(3)},
				},
			}}
		case TemplateCreateQueueV2Query:
			return &recordingQuery{mapScanCASFn: func(map[string]any) (bool, error) {
				return true, nil
			}}
		case TemplateGetQueueV2Query:
			return &recordingQuery{scanFn: func(...any) error { return gocql.ErrNotFound }}
		default:
			return &recordingQuery{}
		}
	}

	result, err := BackfillQueueV2MetadataPage(
		t.Context(),
		session,
		p.QueueTypeHistoryDLQ,
		10,
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 1, result.RowsProcessed)
	require.Equal(t, []byte("next-page"), result.NextPageToken)
	require.Equal(t, []string{
		templateGetQueueNamesQuery,
		TemplateGetQueueV2Query,
		TemplateCreateQueueV2Query,
	}, recordedStatements(session.queries))
}

func marshalQueueV2Metadata(t *testing.T, minMessageID int64) []byte {
	t.Helper()
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {MinMessageId: minMessageID},
		},
	}).Marshal()
	require.NoError(t, err)
	return queueBytes
}
