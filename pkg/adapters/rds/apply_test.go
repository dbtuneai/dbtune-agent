package rds

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsApplyConfigError(t *testing.T) {
	t.Run("preserves the type pg.ValidateRestartPolicy returns", func(t *testing.T) {
		// Wrapping would downgrade this to config_apply_error.
		inner := &agent.RestartNotAllowedError{Message: "restart is not allowed in the agent"}
		got := asApplyConfigError(inner)
		assert.Equal(t, "restart_not_allowed", got.ErrorType())
	})

	t.Run("survives wrapping", func(t *testing.T) {
		inner := &agent.RestartNotAllowedError{Message: "nope"}
		got := asApplyConfigError(fmt.Errorf("failed to modify parameter group: %w", inner))
		assert.Equal(t, "restart_not_allowed", got.ErrorType())
	})

	t.Run("falls back to config_apply_error", func(t *testing.T) {
		got := asApplyConfigError(errors.New("boom"))
		assert.Equal(t, "config_apply_error", got.ErrorType())
		assert.Contains(t, got.Error(), "failed to apply config: boom")
	})
}

func TestAWSParameters(t *testing.T) {
	params := awsParameters(
		[]configInfo{{Name: "work_mem", Value: "16384"}},
		rdsTypes.ApplyMethodImmediate,
	)
	require.Len(t, params, 1)
	assert.Equal(t, "work_mem", aws.ToString(params[0].ParameterName))
	assert.Equal(t, "16384", aws.ToString(params[0].ParameterValue))
	assert.Equal(t, rdsTypes.ApplyMethodImmediate, params[0].ApplyMethod)
}

func TestGetConfigInfo_UnknownKnob(t *testing.T) {
	// The knobs are resolved before the parameter group is read, so an unknown
	// knob fails the whole batch without an AWS call. That ordering is what
	// makes this testable with a zero AWSClients.
	_, err := getConfigInfo(
		&agent.ProposedConfigResponse{
			KnobsOverrides: []string{"work_mem", "nope"},
			Config: []agent.PGConfigRow{
				{Name: "work_mem", Setting: 16384, Vartype: "integer"},
			},
		},
		&AWSClients{},
		"my-pg",
		context.Background(),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find recommended knob")
}

func TestStateCheckApplyDebounced(t *testing.T) {
	assert.False(t, (&State{}).CheckApplyDebounced(time.Minute), "fresh state")
	// Debounced on the attempt, not the success.
	assert.True(t, (&State{LastApplyAttempt: time.Now()}).CheckApplyDebounced(time.Minute))
	assert.False(t, (&State{LastApplyAttempt: time.Now().Add(-2 * time.Minute)}).CheckApplyDebounced(time.Minute))
}
