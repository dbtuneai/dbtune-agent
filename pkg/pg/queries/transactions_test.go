package queries

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionCommitsPayload_JSONOmitsZeroTPS(t *testing.T) {
	payload := TransactionCommitsPayload{
		XactCommit: 42,
		TPS:        0,
	}

	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	assert.Equal(t, float64(42), m["xact_commit"])
	_, hasTPS := m["tps"]
	assert.False(t, hasTPS, "tps should be omitted when zero")
}

func TestTransactionCommitsPayload_JSONIncludesNonZeroTPS(t *testing.T) {
	payload := TransactionCommitsPayload{
		XactCommit: 100,
		TPS:        25.5,
	}

	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	assert.Equal(t, float64(100), m["xact_commit"])
	assert.Equal(t, 25.5, m["tps"])
}

func TestTransactionCommitsPayload_Roundtrip(t *testing.T) {
	original := TransactionCommitsPayload{
		XactCommit: 999,
		TPS:        10.3,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded TransactionCommitsPayload
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}
