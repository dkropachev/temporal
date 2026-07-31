package persistence

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestHistoryManagerPropagatesStoreTokenMetadata(t *testing.T) {
	inputPageState := []byte("input-page-state")
	inputMetadata := []byte("input-metadata")
	outputPageState := []byte("output-page-state")
	outputMetadata := []byte("output-metadata")
	branchRanges := []*persistencespb.HistoryBranchRange{
		{
			BranchId:    "branch-id",
			BeginNodeId: 1,
			EndNodeId:   100,
		},
	}

	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			store := &historyPaginationTestExecutionStore{
				readHistoryBranchFn: func(
					_ context.Context,
					request *InternalReadHistoryBranchRequest,
				) (*InternalReadHistoryBranchResponse, error) {
					require.Equal(t, inputPageState, request.NextPageToken)
					require.Equal(t, inputMetadata, request.NextPageTokenMetadata)
					require.Equal(t, reverse, request.ReverseOrder)
					return &InternalReadHistoryBranchResponse{
						NextPageToken:         outputPageState,
						NextPageTokenMetadata: outputMetadata,
					}, nil
				},
			}
			manager := &executionManagerImpl{persistence: store}
			token := &historyPagingToken{
				StoreToken:         inputPageState,
				StoreTokenMetadata: inputMetadata,
				CurrentRangeIndex:  0,
				FinalRangeIndex:    0,
			}

			var resultToken *historyPagingToken
			var err error
			if reverse {
				_, resultToken, err = manager.readRawHistoryBranchReverse(
					t.Context(),
					[]byte("branch-token"),
					1,
					"tree-id",
					branchRanges,
					1,
					100,
					token,
					10,
					false,
				)
			} else {
				_, resultToken, err = manager.readRawHistoryBranch(
					t.Context(),
					[]byte("branch-token"),
					1,
					branchRanges,
					1,
					100,
					token,
					10,
					false,
				)
			}

			require.NoError(t, err)
			require.Equal(t, outputPageState, resultToken.StoreToken)
			require.Equal(t, outputMetadata, resultToken.StoreTokenMetadata)
		})
	}
}

func TestHistoryManagerClearsStoreTokenMetadataWithoutPageState(t *testing.T) {
	manager := &executionManagerImpl{
		pagingTokenSerializer: newJSONHistoryTokenSerializer(),
	}
	token := &historyPagingToken{
		StoreTokenMetadata: []byte("stale-metadata"),
		CurrentRangeIndex:  0,
		FinalRangeIndex:    1,
	}

	serialized, err := manager.serializeToken(token, false)

	require.NoError(t, err)
	require.Nil(t, token.StoreTokenMetadata)
	roundTripped, err := manager.deserializeToken(serialized, 0, 0)
	require.NoError(t, err)
	require.Nil(t, roundTripped.StoreTokenMetadata)
	require.Equal(t, 1, roundTripped.CurrentRangeIndex)
}

type historyPaginationTestExecutionStore struct {
	ExecutionStore
	readHistoryBranchFn func(
		context.Context,
		*InternalReadHistoryBranchRequest,
	) (*InternalReadHistoryBranchResponse, error)
}

func (s *historyPaginationTestExecutionStore) ReadHistoryBranch(
	ctx context.Context,
	request *InternalReadHistoryBranchRequest,
) (*InternalReadHistoryBranchResponse, error) {
	return s.readHistoryBranchFn(ctx, request)
}
