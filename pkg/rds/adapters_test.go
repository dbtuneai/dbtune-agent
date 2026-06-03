package rds

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultParameterGroupError(t *testing.T) {
	t.Run("nil DBInfo", func(t *testing.T) {
		assert.Nil(t, defaultParameterGroupError(nil))
	})
	t.Run("empty name", func(t *testing.T) {
		assert.Nil(t, defaultParameterGroupError(&DBInfo{ParameterGroupName: ""}))
	})
	t.Run("custom group", func(t *testing.T) {
		assert.Nil(t, defaultParameterGroupError(&DBInfo{ParameterGroupName: "my-pg"}))
	})
	t.Run("default group", func(t *testing.T) {
		err := defaultParameterGroupError(&DBInfo{ParameterGroupName: "default.postgres15"})
		require.NotNil(t, err)
		assert.Equal(t, "default.postgres15", err.ParameterGroupName)
	})
	t.Run("default prefix substring is not enough", func(t *testing.T) {
		// only the "default." prefix triggers; names that merely contain it do not.
		assert.Nil(t, defaultParameterGroupError(&DBInfo{ParameterGroupName: "my-default.postgres15"}))
	})
}

func TestDBInfo_TryIntoFlatValuesSlice_IncludesParameterGroups(t *testing.T) {
	t.Run("instance only", func(t *testing.T) {
		info := &DBInfo{ParameterGroupName: "my-pg"}
		flats, err := info.TryIntoFlatValuesSlice()
		require.NoError(t, err)
		keys := flatKeys(flats)
		assert.Contains(t, keys, "aws_rds_parameter_group")
		assert.NotContains(t, keys, "aws_rds_cluster_parameter_group")
	})
	t.Run("instance and cluster", func(t *testing.T) {
		info := &DBInfo{
			ParameterGroupName:        "my-pg",
			ClusterParameterGroupName: "my-cluster-pg",
		}
		flats, err := info.TryIntoFlatValuesSlice()
		require.NoError(t, err)
		keys := flatKeys(flats)
		assert.Contains(t, keys, "aws_rds_parameter_group")
		assert.Contains(t, keys, "aws_rds_cluster_parameter_group")
	})
	t.Run("neither when empty", func(t *testing.T) {
		info := &DBInfo{}
		flats, err := info.TryIntoFlatValuesSlice()
		require.NoError(t, err)
		keys := flatKeys(flats)
		assert.NotContains(t, keys, "aws_rds_parameter_group")
		assert.NotContains(t, keys, "aws_rds_cluster_parameter_group")
	})
}

func flatKeys(fs []metrics.FlatValue) []string {
	keys := make([]string, 0, len(fs))
	for _, f := range fs {
		keys = append(keys, f.Key)
	}
	return keys
}

func TestRDSAdapter_ApplyConfig_RefusesDefaultParameterGroup(t *testing.T) {
	adapter := &RDSAdapter{
		State: State{
			DBInfo: &DBInfo{ParameterGroupName: "default.postgres15"},
		},
	}

	err := adapter.ApplyConfig(context.Background(), &agent.ProposedConfigResponse{})
	require.NotNil(t, err, "expected typed apply error, got nil")

	var typed *agent.DefaultParameterGroupError
	require.True(t, errors.As(err, &typed), "expected *DefaultParameterGroupError, got %T", err)
	assert.Equal(t, "default.postgres15", typed.ParameterGroupName)
	assert.Equal(t, "default_parameter_group", err.ErrorType())
}
