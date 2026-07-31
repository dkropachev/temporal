package cassandra

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
)

const (
	historyNodePageTokenMetadataVersion       = byte(2)
	historyNodeLegacyPageTokenMetadataVersion = byte(1)
	historyNodePageTokenMetadataLength        = 18
	// Nine 0xff bytes are invalid in both Cassandra's legacy-short and modern-vint paging-state formats.
	historyNodePageTokenEnvelopePrefix       = "\xff\xff\xff\xff\xff\xff\xff\xff\xffTEMPORAL-HISTORY-PAGE"
	historyNodePageTokenEnvelopeVersion      = byte(1)
	historyNodePageTokenEnvelopeHeaderLength = len(historyNodePageTokenEnvelopePrefix) + 1 + 4

	// below are templates for history_node table
	v2templateUpsertHistoryNode = `INSERT INTO history_node (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) `

	v2templateReadHistoryNode = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateReadHistoryNodeReverse = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? ORDER BY branch_id DESC, node_id DESC `

	v2templateReadHistoryNodeReverseOldV2 = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? ORDER BY node_id DESC `

	v2templateReadHistoryNodeMetadata = `SELECT node_id, prev_txn_id, txn_id FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateDeleteHistoryNode = `DELETE FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ? `

	v2templateRangeDeleteHistoryNode = `DELETE FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id >= ? `

	v2templateUpsertHistoryNodeV2 = `INSERT INTO history_node_v2 (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) `

	v2templateReadHistoryNodeV2 = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node_v2 ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateReadHistoryNodeReverseV2 = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node_v2 ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? ORDER BY node_id DESC `

	v2templateReadHistoryNodeMetadataV2 = `SELECT node_id, prev_txn_id, txn_id FROM history_node_v2 ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateDeleteHistoryNodeV2 = `DELETE FROM history_node_v2 WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ? `

	v2templateRangeDeleteHistoryNodeV2 = `DELETE FROM history_node_v2 WHERE tree_id = ? AND branch_id = ? AND node_id >= ? `

	// below are templates for history_tree table
	v2templateInsertTree = `INSERT INTO history_tree (` +
		`tree_id, branch_id, branch, branch_encoding) ` +
		`VALUES (?, ?, ?, ?) `

	v2templateReadAllBranches = `SELECT branch_id, branch, branch_encoding FROM history_tree WHERE tree_id = ? `

	v2templateDeleteBranch = `DELETE FROM history_tree WHERE tree_id = ? AND branch_id = ? `

	v2templateScanAllTreeBranches = `SELECT tree_id, branch_id, branch, branch_encoding FROM history_tree `
)

type (
	HistoryStore struct {
		Session gocql.Session
		p.HistoryBranchUtil
		historyNodeMigrationMode config.CassandraHistoryNodeMigrationMode
		historyNodeGenerations   historyNodeTableGenerations
	}

	historyNodeMutationPlan struct {
		primaryQuery        string
		mirrorQuery         string
		optionalMirrorTable string
	}

	historyNodeReadLayout byte
)

const (
	historyNodeReadLayoutLegacyV1 historyNodeReadLayout = iota + 1
	historyNodeReadLayoutOldV2
	historyNodeReadLayoutCanonicalV2
)

func NewHistoryStore(
	session gocql.Session,
	serializer serialization.Serializer,
	historyNodeMigrationMode ...config.CassandraHistoryNodeMigrationMode,
) *HistoryStore {
	mode := config.CassandraHistoryNodeMigrationModeLegacyV1Dual
	if len(historyNodeMigrationMode) > 0 {
		mode = normalizeHistoryNodeMigrationMode(historyNodeMigrationMode[0])
	}
	return &HistoryStore{
		Session:                  session,
		HistoryBranchUtil:        p.NewHistoryBranchUtil(serializer),
		historyNodeMigrationMode: mode,
	}
}

func newHistoryStore(
	session gocql.Session,
	serializer serialization.Serializer,
	mode config.CassandraHistoryNodeMigrationMode,
	generations historyNodeTableGenerations,
) *HistoryStore {
	store := NewHistoryStore(session, serializer, mode)
	store.historyNodeGenerations = generations
	return store
}

func (h *HistoryStore) historyNodeMutationPlan(
	legacyQuery string,
	v2Query string,
) (historyNodeMutationPlan, error) {
	switch h.historyNodeMigrationMode {
	case config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
		config.CassandraHistoryNodeMigrationModeOldV2RebuildV2:
		return historyNodeMutationPlan{
			primaryQuery:        legacyQuery,
			mirrorQuery:         v2Query,
			optionalMirrorTable: historyNodeV2TableName,
		}, nil
	case config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
		config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
		config.CassandraHistoryNodeMigrationModeOldV2Dual,
		config.CassandraHistoryNodeMigrationModeV1CutoverDual:
		return historyNodeMutationPlan{
			primaryQuery: legacyQuery,
			mirrorQuery:  v2Query,
		}, nil
	case config.CassandraHistoryNodeMigrationModeV1RebuildDual:
		return historyNodeMutationPlan{
			primaryQuery:        v2Query,
			mirrorQuery:         legacyQuery,
			optionalMirrorTable: historyNodeTableName,
		}, nil
	case config.CassandraHistoryNodeMigrationModeLegacyV1CutoverDual,
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual,
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
		config.CassandraHistoryNodeMigrationModeCanonicalDual:
		return historyNodeMutationPlan{
			primaryQuery: v2Query,
			mirrorQuery:  legacyQuery,
		}, nil
	case config.CassandraHistoryNodeMigrationModeV2Only:
		return historyNodeMutationPlan{primaryQuery: v2Query}, nil
	default:
		return historyNodeMutationPlan{}, fmt.Errorf(
			"unsupported Cassandra history node migration mode %q",
			h.historyNodeMigrationMode,
		)
	}
}

func (h *HistoryStore) historyNodeMutationQueries(
	legacyQuery string,
	v2Query string,
) ([]string, error) {
	plan, err := h.historyNodeMutationPlan(legacyQuery, v2Query)
	if err != nil {
		return nil, err
	}
	queries := []string{plan.primaryQuery}
	if plan.mirrorQuery != "" {
		queries = append(queries, plan.mirrorQuery)
	}
	return queries, nil
}

func (h *HistoryStore) historyNodeReadLayout() (historyNodeReadLayout, error) {
	switch h.historyNodeMigrationMode {
	case config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
		config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
		config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
		config.CassandraHistoryNodeMigrationModeLegacyV1CutoverDual:
		return historyNodeReadLayoutLegacyV1, nil
	case config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
		config.CassandraHistoryNodeMigrationModeOldV2Dual,
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual:
		return historyNodeReadLayoutOldV2, nil
	case config.CassandraHistoryNodeMigrationModeOldV2CutoverDual,
		config.CassandraHistoryNodeMigrationModeV1RebuildDual,
		config.CassandraHistoryNodeMigrationModeV1CutoverDual,
		config.CassandraHistoryNodeMigrationModeV2Only,
		config.CassandraHistoryNodeMigrationModeCanonicalDual:
		return historyNodeReadLayoutCanonicalV2, nil
	default:
		return 0, fmt.Errorf(
			"unsupported Cassandra history node migration mode %q",
			h.historyNodeMigrationMode,
		)
	}
}

func (h *HistoryStore) historyNodeReadQuery(metadataOnly bool, reverseOrder bool) (string, error) {
	layout, err := h.historyNodeReadLayout()
	if err != nil {
		return "", err
	}
	return historyNodeReadQueryForLayout(layout, metadataOnly, reverseOrder)
}

func historyNodeReadQueryForLayout(
	layout historyNodeReadLayout,
	metadataOnly bool,
	reverseOrder bool,
) (string, error) {
	switch layout {
	case historyNodeReadLayoutLegacyV1:
		switch {
		case metadataOnly:
			return v2templateReadHistoryNodeMetadata, nil
		case reverseOrder:
			return v2templateReadHistoryNodeReverse, nil
		default:
			return v2templateReadHistoryNode, nil
		}
	case historyNodeReadLayoutOldV2:
		switch {
		case metadataOnly:
			return v2templateReadHistoryNodeMetadata, nil
		case reverseOrder:
			return v2templateReadHistoryNodeReverseOldV2, nil
		default:
			return v2templateReadHistoryNode, nil
		}
	case historyNodeReadLayoutCanonicalV2:
		switch {
		case metadataOnly:
			return v2templateReadHistoryNodeMetadataV2, nil
		case reverseOrder:
			return v2templateReadHistoryNodeReverseV2, nil
		default:
			return v2templateReadHistoryNodeV2, nil
		}
	default:
		return "", fmt.Errorf(
			"unsupported Cassandra history node read layout %d",
			layout,
		)
	}
}

func (h *HistoryStore) encodeHistoryNodePageTokenMetadata(layout historyNodeReadLayout) []byte {
	generation := h.historyNodeGenerationForLayout(layout)
	if generation == ([16]byte{}) {
		return []byte{historyNodeLegacyPageTokenMetadataVersion, byte(layout)}
	}
	metadata := make([]byte, historyNodePageTokenMetadataLength)
	metadata[0] = historyNodePageTokenMetadataVersion
	metadata[1] = byte(layout)
	copy(metadata[2:], generation[:])
	return metadata
}

func (h *HistoryStore) decodeHistoryNodePageTokenMetadata(
	pageState []byte,
	metadata []byte,
) (historyNodeReadLayout, error) {
	configuredLayout, err := h.historyNodeReadLayout()
	if err != nil {
		return 0, err
	}
	if len(metadata) == 0 {
		if len(pageState) == 0 {
			return configuredLayout, nil
		}
		if h.acceptsRawHistoryNodePageToken() {
			return configuredLayout, nil
		}
		return 0, &p.InvalidPersistenceRequestError{
			Msg: "history node page token has no source-layout metadata; restart pagination",
		}
	}
	if len(pageState) == 0 {
		return 0, &p.InvalidPersistenceRequestError{
			Msg: "invalid history node page token metadata without Cassandra page state",
		}
	}
	version := metadata[0]
	switch version {
	case historyNodeLegacyPageTokenMetadataVersion:
		if len(metadata) != 2 {
			return 0, &p.InvalidPersistenceRequestError{
				Msg: fmt.Sprintf(
					"invalid history node page token metadata length %d",
					len(metadata),
				),
			}
		}
	case historyNodePageTokenMetadataVersion:
		if len(metadata) != historyNodePageTokenMetadataLength {
			return 0, &p.InvalidPersistenceRequestError{
				Msg: fmt.Sprintf(
					"invalid history node page token metadata length %d",
					len(metadata),
				),
			}
		}
	default:
		return 0, &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("invalid history node page token metadata version %d", version),
		}
	}

	layout := historyNodeReadLayout(metadata[1])
	switch layout {
	case historyNodeReadLayoutLegacyV1,
		historyNodeReadLayoutOldV2,
		historyNodeReadLayoutCanonicalV2:
	default:
		return 0, &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("invalid history node page token metadata read layout %d", layout),
		}
	}
	expectedGeneration := h.historyNodeGenerationForLayout(layout)
	if version == historyNodeLegacyPageTokenMetadataVersion && expectedGeneration != ([16]byte{}) {
		return 0, &p.InvalidPersistenceRequestError{
			Msg: "history node page token has no table generation; restart pagination",
		}
	}
	if version == historyNodePageTokenMetadataVersion &&
		!slices.Equal(metadata[2:], expectedGeneration[:]) {
		return 0, &p.InvalidPersistenceRequestError{
			Msg: "history node page token refers to a different table generation; restart pagination",
		}
	}
	if err := h.validateHistoryNodeContinuationLayout(layout); err != nil {
		return 0, err
	}
	return layout, nil
}

func encodeHistoryNodePageState(
	pageState []byte,
	layout historyNodeReadLayout,
) []byte {
	if len(pageState) == 0 || layout != historyNodeReadLayoutCanonicalV2 {
		return pageState
	}

	envelopeLength := historyNodePageTokenEnvelopeHeaderLength + len(pageState)
	envelope := make([]byte, envelopeLength)
	offset := copy(envelope, historyNodePageTokenEnvelopePrefix)
	envelope[offset] = historyNodePageTokenEnvelopeVersion
	offset++
	binary.BigEndian.PutUint32(envelope[offset:], uint32(len(pageState)))
	offset += 4
	copy(envelope[offset:], pageState)
	return envelope
}

func decodeHistoryNodePageState(
	pageToken []byte,
	layout historyNodeReadLayout,
) ([]byte, error) {
	if len(pageToken) == 0 || layout != historyNodeReadLayoutCanonicalV2 {
		return pageToken, nil
	}
	prefix := []byte(historyNodePageTokenEnvelopePrefix)
	if !bytes.HasPrefix(pageToken, prefix) {
		return nil, &p.InvalidPersistenceRequestError{
			Msg: "canonical history node page token is not guarded; restart pagination",
		}
	}
	if len(pageToken) < historyNodePageTokenEnvelopeHeaderLength {
		return nil, &p.InvalidPersistenceRequestError{
			Msg: "canonical history node page token envelope is truncated; restart pagination",
		}
	}

	offset := len(prefix)
	if pageToken[offset] != historyNodePageTokenEnvelopeVersion {
		return nil, &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf(
				"canonical history node page token envelope version %d is invalid; restart pagination",
				pageToken[offset],
			),
		}
	}
	offset++
	pageStateLength := int(binary.BigEndian.Uint32(pageToken[offset:]))
	offset += 4
	if pageStateLength == 0 || pageStateLength != len(pageToken)-offset {
		return nil, &p.InvalidPersistenceRequestError{
			Msg: "canonical history node page token envelope length is invalid; restart pagination",
		}
	}
	return pageToken[offset:], nil
}

func (h *HistoryStore) historyNodeGenerationForLayout(layout historyNodeReadLayout) [16]byte {
	switch layout {
	case historyNodeReadLayoutLegacyV1, historyNodeReadLayoutOldV2:
		return h.historyNodeGenerations.historyNode
	case historyNodeReadLayoutCanonicalV2:
		return h.historyNodeGenerations.historyNodeV2
	default:
		return [16]byte{}
	}
}

func (h *HistoryStore) validateHistoryNodeContinuationLayout(layout historyNodeReadLayout) error {
	var safeLayouts []historyNodeReadLayout
	switch h.historyNodeMigrationMode {
	case config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
		config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
		config.CassandraHistoryNodeMigrationModeLegacyV1RollbackDual,
		config.CassandraHistoryNodeMigrationModeLegacyV1CutoverDual:
		safeLayouts = []historyNodeReadLayout{
			historyNodeReadLayoutLegacyV1,
		}
		if h.historyNodeMigrationMode != config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2 {
			safeLayouts = append(safeLayouts, historyNodeReadLayoutCanonicalV2)
		}
	case config.CassandraHistoryNodeMigrationModeOldV2RebuildV2:
		safeLayouts = []historyNodeReadLayout{historyNodeReadLayoutOldV2}
	case config.CassandraHistoryNodeMigrationModeOldV2Dual,
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual,
		config.CassandraHistoryNodeMigrationModeOldV2CutoverDual:
		safeLayouts = []historyNodeReadLayout{
			historyNodeReadLayoutOldV2,
			historyNodeReadLayoutCanonicalV2,
		}
	case config.CassandraHistoryNodeMigrationModeV1RebuildDual,
		config.CassandraHistoryNodeMigrationModeV2Only:
		safeLayouts = []historyNodeReadLayout{historyNodeReadLayoutCanonicalV2}
	case config.CassandraHistoryNodeMigrationModeV1CutoverDual,
		config.CassandraHistoryNodeMigrationModeCanonicalDual:
		safeLayouts = []historyNodeReadLayout{
			historyNodeReadLayoutLegacyV1,
			historyNodeReadLayoutCanonicalV2,
		}
	default:
		return fmt.Errorf(
			"unsupported Cassandra history node migration mode %q",
			h.historyNodeMigrationMode,
		)
	}
	if !slices.Contains(safeLayouts, layout) {
		return &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf(
				"history node page token layout %d is unsafe in migration mode %q; restart pagination",
				layout,
				h.historyNodeMigrationMode,
			),
		}
	}
	return nil
}

func (h *HistoryStore) acceptsRawHistoryNodePageToken() bool {
	switch h.historyNodeMigrationMode {
	case config.CassandraHistoryNodeMigrationModeLegacyV1RebuildV2,
		config.CassandraHistoryNodeMigrationModeLegacyV1Dual,
		config.CassandraHistoryNodeMigrationModeLegacyV1CutoverDual,
		config.CassandraHistoryNodeMigrationModeOldV2RebuildV2,
		config.CassandraHistoryNodeMigrationModeOldV2Dual,
		config.CassandraHistoryNodeMigrationModeOldV2PrepareCutoverDual:
		return true
	default:
		return false
	}
}

func (h *HistoryStore) executeHistoryNodeMutation(
	ctx context.Context,
	plan historyNodeMutationPlan,
	addSharedQueries func(*gocql.Batch),
	addHistoryNodeQueries func(*gocql.Batch, string) bool,
) error {
	batch := h.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	if addSharedQueries != nil {
		addSharedQueries(batch)
	}
	hasHistoryNodeQueries := addHistoryNodeQueries(batch, plan.primaryQuery)
	if hasHistoryNodeQueries && plan.mirrorQuery != "" {
		addHistoryNodeQueries(batch, plan.mirrorQuery)
	}
	return h.Session.ExecuteBatch(batch)
}

// AppendHistoryNodes upsert a batch of events as a single node to a history branch
// Note that it's not allowed to append above the branch's ancestors' nodes, which means nodeID >= ForkNodeID
func (h *HistoryStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	plan, err := h.historyNodeMutationPlan(
		v2templateUpsertHistoryNode,
		v2templateUpsertHistoryNodeV2,
	)
	if err != nil {
		return err
	}

	if err := h.executeHistoryNodeUpserts(ctx, plan, request); err != nil {
		return convertTimeoutError(gocql.ConvertError("AppendHistoryNodes", err))
	}
	return nil
}

func (h *HistoryStore) executeHistoryNodeUpserts(
	ctx context.Context,
	plan historyNodeMutationPlan,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	branchInfo := request.BranchInfo
	node := request.Node
	timestamp := time.Now().UnixMicro()

	if request.IsNewBranch {
		treeInfoDataBlob := request.TreeInfo
		primaryBatch := h.Session.NewBatch(gocql.LoggedBatch).
			WithContext(ctx).
			WithTimestamp(timestamp)
		primaryBatch.Query(
			v2templateInsertTree,
			branchInfo.TreeId,
			branchInfo.BranchId,
			treeInfoDataBlob.Data,
			treeInfoDataBlob.EncodingType.String(),
		)
		h.addHistoryNodeUpsert(primaryBatch, plan.primaryQuery, branchInfo, node)
		if err := h.Session.ExecuteBatch(primaryBatch); err != nil {
			return err
		}
	} else if err := h.executeHistoryNodeUpsert(
		ctx,
		plan.primaryQuery,
		branchInfo,
		node,
		timestamp,
	); err != nil {
		return err
	}

	if plan.mirrorQuery == "" {
		return nil
	}

	// Keep the large event blob out of a cross-table batch. Workflow mutations
	// append history before mutable state, so a required mirror failure prevents
	// the state commit and retries can complete these idempotent inserts.
	if err := h.executeHistoryNodeUpsert(
		ctx,
		plan.mirrorQuery,
		branchInfo,
		node,
		timestamp,
	); err != nil &&
		!gocql.IsUnconfiguredTableError(err, plan.optionalMirrorTable) {
		return err
	}
	return nil
}

func (h *HistoryStore) executeHistoryNodeUpsert(
	ctx context.Context,
	query string,
	branchInfo *persistencespb.HistoryBranch,
	node p.InternalHistoryNode,
	timestamp int64,
) error {
	return h.Session.Query(
		query,
		branchInfo.TreeId,
		branchInfo.BranchId,
		node.NodeID,
		node.PrevTransactionID,
		node.TransactionID,
		node.Events.Data,
		node.Events.EncodingType.String(),
	).WithContext(ctx).WithTimestamp(timestamp).Exec()
}

func (h *HistoryStore) addHistoryNodeUpsert(
	batch *gocql.Batch,
	query string,
	branchInfo *persistencespb.HistoryBranch,
	node p.InternalHistoryNode,
) {
	batch.Query(query,
		branchInfo.TreeId,
		branchInfo.BranchId,
		node.NodeID,
		node.PrevTransactionID,
		node.TransactionID,
		node.Events.Data,
		node.Events.EncodingType.String(),
	)
}

// DeleteHistoryNodes delete a history node
func (h *HistoryStore) DeleteHistoryNodes(
	ctx context.Context,
	request *p.InternalDeleteHistoryNodesRequest,
) error {
	branchInfo := request.BranchInfo
	treeID := branchInfo.TreeId
	branchID := branchInfo.BranchId
	nodeID := request.NodeID
	txnID := request.TransactionID

	if nodeID < p.GetBeginNodeID(branchInfo) {
		return &p.InvalidPersistenceRequestError{
			Msg: "cannot delete from ancestors' nodes",
		}
	}

	plan, err := h.historyNodeMutationPlan(
		v2templateDeleteHistoryNode,
		v2templateDeleteHistoryNodeV2,
	)
	if err != nil {
		return err
	}
	if err := h.executeHistoryNodeMutation(ctx, plan, nil, func(batch *gocql.Batch, query string) bool {
		batch.Query(query,
			treeID,
			branchID,
			nodeID,
			txnID,
		)
		return true
	}); err != nil {
		return gocql.ConvertError("DeleteHistoryNodes", err)
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
// NOTE: For branch that has ancestors, we need to query Cassandra multiple times, because it doesn't support OR/UNION operator
func (h *HistoryStore) ReadHistoryBranch(
	ctx context.Context,
	request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {
	branch, err := h.ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	treeID, err := primitives.ValidateUUID(branch.TreeId)
	if err != nil {
		return nil, serviceerror.NewInternalf("ReadHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err)
	}

	branchID, err := primitives.ValidateUUID(request.BranchID)
	if err != nil {
		return nil, serviceerror.NewInternalf("ReadHistoryBranch - Gocql BranchId UUID cast failed. Error: %v", err)
	}

	readLayout, err := h.decodeHistoryNodePageTokenMetadata(
		request.NextPageToken,
		request.NextPageTokenMetadata,
	)
	if err != nil {
		return nil, err
	}
	pageState, err := decodeHistoryNodePageState(request.NextPageToken, readLayout)
	if err != nil {
		return nil, err
	}

	queryString, err := historyNodeReadQueryForLayout(
		readLayout,
		request.MetadataOnly,
		request.ReverseOrder,
	)
	if err != nil {
		return nil, err
	}

	query := h.Session.Query(queryString, treeID, branchID, request.MinNodeID, request.MaxNodeID).WithContext(ctx)

	iter := query.PageSize(request.PageSize).PageState(pageState).Iter()

	nodes := make([]p.InternalHistoryNode, 0, iter.NumRows())
	var nodeID int64
	var prevTxnID int64
	var txnID int64
	var data []byte
	var dataEncoding string
	scanDestinations := []any{&nodeID, &prevTxnID, &txnID}
	if !request.MetadataOnly {
		scanDestinations = append(scanDestinations, &data, &dataEncoding)
	}
	for iter.Scan(scanDestinations...) {
		nodes = append(nodes, p.InternalHistoryNode{
			NodeID:            nodeID,
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
			Events:            p.NewDataBlob(data, dataEncoding),
		})

		nodeID = 0
		prevTxnID = 0
		txnID = 0
		data = nil
		dataEncoding = ""
	}

	pagingToken := iter.PageState()
	var pagingTokenMetadata []byte
	if len(pagingToken) > 0 {
		pagingTokenMetadata = h.encodeHistoryNodePageTokenMetadata(readLayout)
		pagingToken = encodeHistoryNodePageState(pagingToken, readLayout)
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ReadHistoryBranch", err)
	}

	return &p.InternalReadHistoryBranchResponse{
		Nodes:                 nodes,
		NextPageToken:         pagingToken,
		NextPageTokenMetadata: pagingTokenMetadata,
	}, nil
}

// ForkHistoryBranch forks a new branch from an existing branch
// Note that application must provide a void forking nodeID, it must be a valid nodeID in that branch.
// A valid forking nodeID can be an ancestor from the existing branch.
// For example, we have branch B1 with three nodes(1[1,2], 3[3,4,5] and 6[6,7,8]. 1, 3 and 6 are nodeIDs (first eventID of the batch).
// So B1 looks like this:
//
//	     1[1,2]
//	     /
//	   3[3,4,5]
//	  /
//	6[6,7,8]
//
// Assuming we have branch B2 which contains one ancestor B1 stopping at 6 (exclusive). So B2 inherit nodeID 1 and 3 from B1, and have its own nodeID 6 and 8.
// Branch B2 looks like this:
//
//	  1[1,2]
//	  /
//	3[3,4,5]
//	 \
//	  6[6,7]
//	  \
//	   8[8]
//
// Now we want to fork a new branch B3 from B2.
// The only valid forking nodeIDs are 3,6 or 8.
// 1 is not valid because we can't fork from first node.
// 2/4/5 is NOT valid either because they are inside a batch.
//
// Case #1: If we fork from nodeID 6, then B3 will have an ancestor B1 which stops at 6(exclusive).
// As we append a batch of events[6,7,8,9] to B3, it will look like :
//
//	  1[1,2]
//	  /
//	3[3,4,5]
//	 \
//	6[6,7,8,9]
//
// Case #2: If we fork from node 8, then B3 will have two ancestors: B1 stops at 6(exclusive) and ancestor B2 stops at 8(exclusive)
// As we append a batch of events[8,9] to B3, it will look like:
//
//	     1[1,2]
//	     /
//	   3[3,4,5]
//	  /
//	6[6,7]
//	 \
//	 8[8,9]
func (h *HistoryStore) ForkHistoryBranch(
	ctx context.Context,
	request *p.InternalForkHistoryBranchRequest,
) error {

	forkB := request.ForkBranchInfo
	datablob := request.TreeInfo

	cqlTreeID, err := primitives.ValidateUUID(forkB.TreeId)
	if err != nil {
		return serviceerror.NewInternalf("ForkHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err)
	}

	cqlNewBranchID, err := primitives.ValidateUUID(request.NewBranchID)
	if err != nil {
		return serviceerror.NewInternalf("ForkHistoryBranch - Gocql NewBranchID UUID cast failed. Error: %v", err)
	}
	query := h.Session.Query(v2templateInsertTree, cqlTreeID, cqlNewBranchID, datablob.Data, datablob.EncodingType.String()).WithContext(ctx)
	err = query.Exec()
	if err != nil {
		return gocql.ConvertError("ForkHistoryBranch", err)
	}

	return nil
}

// DeleteHistoryBranch removes a branch
func (h *HistoryStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {

	plan, err := h.historyNodeMutationPlan(
		v2templateRangeDeleteHistoryNode,
		v2templateRangeDeleteHistoryNodeV2,
	)
	if err != nil {
		return err
	}
	for _, br := range request.BranchRanges {
		err = h.executeHistoryNodeMutation(
			ctx,
			plan,
			nil,
			func(batch *gocql.Batch, query string) bool {
				batch.Query(
					query,
					request.BranchInfo.TreeId,
					br.BranchId,
					br.BeginNodeId,
				)
				return true
			},
		)
		if err != nil {
			return gocql.ConvertError("DeleteHistoryBranch", err)
		}
	}

	err = h.Session.Query(
		v2templateDeleteBranch,
		request.BranchInfo.TreeId,
		request.BranchInfo.BranchId,
	).WithContext(ctx).Exec()
	if err != nil {
		return gocql.ConvertError("DeleteHistoryBranch", err)
	}
	return nil
}

func (h *HistoryStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {

	query := h.Session.Query(v2templateScanAllTreeBranches).WithContext(ctx)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	branches := make([]p.InternalHistoryBranchDetail, 0, preallocatedResultCapacity(request.PageSize))
	treeUUID := ""
	branchUUID := ""
	var data []byte
	var encoding string

	for iter.Scan(&treeUUID, &branchUUID, &data, &encoding) {
		branch := p.InternalHistoryBranchDetail{
			TreeID:   treeUUID,
			BranchID: branchUUID,
			Data:     data,
			Encoding: encoding,
		}
		branches = append(branches, branch)

		treeUUID = ""
		branchUUID = ""
		data = nil
		encoding = ""
	}

	var pagingToken []byte
	if len(iter.PageState()) > 0 {
		pagingToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetAllHistoryTreeBranches", err)
	}

	response := &p.InternalGetAllHistoryTreeBranchesResponse{
		Branches:      branches,
		NextPageToken: pagingToken,
	}

	return response, nil
}

// GetHistoryTreeContainingBranch returns all branch information of a tree
func (h *HistoryStore) GetHistoryTreeContainingBranch(
	ctx context.Context,
	request *p.InternalGetHistoryTreeContainingBranchRequest,
) (*p.InternalGetHistoryTreeContainingBranchResponse, error) {

	branch, err := h.ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	treeID, err := primitives.ValidateUUID(branch.TreeId)
	if err != nil {
		return nil, serviceerror.NewInternalf("ReadHistoryBranch. Gocql TreeId UUID cast failed. Error: %v", err)
	}
	query := h.Session.Query(v2templateReadAllBranches, treeID).WithContext(ctx)

	pageSize := 100
	var pagingToken []byte
	treeInfos := make([]*commonpb.DataBlob, 0, pageSize)

	var iter gocql.Iter
	for {
		iter = query.
			PageSize(pageSize).
			PageState(pagingToken).
			Iter()

		branchUUID := ""
		var data []byte
		var encoding string
		for iter.Scan(&branchUUID, &data, &encoding) {
			treeInfos = append(treeInfos, p.NewDataBlob(data, encoding))

			branchUUID = ""
			data = []byte{}
			encoding = ""
		}

		nextPagingToken := iter.PageState()
		if err := iter.Close(); err != nil {
			return nil, gocql.ConvertError("GetHistoryTree", err)
		}

		if len(nextPagingToken) == 0 {
			break
		}
		pagingToken = nextPagingToken
	}

	return &p.InternalGetHistoryTreeContainingBranchResponse{TreeInfos: treeInfos}, nil
}

func (h *HistoryStore) GetHistoryBranchUtil() p.HistoryBranchUtil {
	return h.HistoryBranchUtil
}

func convertTimeoutError(err error) error {
	if timeoutErr, ok := err.(*p.TimeoutError); ok {
		return &p.AppendHistoryTimeoutError{
			Msg: timeoutErr.Msg,
		}
	}
	return err
}
