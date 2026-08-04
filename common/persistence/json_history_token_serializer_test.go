package persistence

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONHistoryTokenSerializerStoreTokenMetadataCompatibility(t *testing.T) {
	type legacyHistoryPagingToken struct {
		LastEventID       int64
		StoreToken        []byte
		CurrentRangeIndex int
		FinalRangeIndex   int
		LastNodeID        int64
		LastTransactionID int64
	}

	serializer := newJSONHistoryTokenSerializer()
	token := &historyPagingToken{
		LastEventID:        11,
		StoreToken:         []byte{0, 1, 2, 255},
		StoreTokenMetadata: []byte{3, 4, 5, 254},
		CurrentRangeIndex:  1,
		FinalRangeIndex:    2,
		LastNodeID:         10,
		LastTransactionID:  12,
	}

	serialized, err := serializer.Serialize(token)
	require.NoError(t, err)

	roundTripped, err := serializer.Deserialize(serialized, 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, token, roundTripped)

	var legacyToken legacyHistoryPagingToken
	require.NoError(t, json.Unmarshal(serialized, &legacyToken))
	require.Equal(t, token.LastEventID, legacyToken.LastEventID)
	require.Equal(t, token.StoreToken, legacyToken.StoreToken)
	require.Equal(t, token.CurrentRangeIndex, legacyToken.CurrentRangeIndex)
	require.Equal(t, token.FinalRangeIndex, legacyToken.FinalRangeIndex)
	require.Equal(t, token.LastNodeID, legacyToken.LastNodeID)
	require.Equal(t, token.LastTransactionID, legacyToken.LastTransactionID)

	legacySerialized, err := json.Marshal(legacyToken)
	require.NoError(t, err)
	require.NotContains(t, string(legacySerialized), "StoreTokenMetadata")

	fromLegacy, err := serializer.Deserialize(legacySerialized, 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, token.StoreToken, fromLegacy.StoreToken)
	require.Nil(t, fromLegacy.StoreTokenMetadata)
}

func TestJSONHistoryTokenSerializerOmitsEmptyStoreTokenMetadata(t *testing.T) {
	serializer := newJSONHistoryTokenSerializer()

	for _, metadata := range [][]byte{nil, {}} {
		serialized, err := serializer.Serialize(&historyPagingToken{
			StoreToken:         []byte("raw-page-state"),
			StoreTokenMetadata: metadata,
		})
		require.NoError(t, err)
		require.NotContains(t, string(serialized), "StoreTokenMetadata")

		roundTripped, err := serializer.Deserialize(serialized, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, []byte("raw-page-state"), roundTripped.StoreToken)
		require.Empty(t, roundTripped.StoreTokenMetadata)
	}
}
