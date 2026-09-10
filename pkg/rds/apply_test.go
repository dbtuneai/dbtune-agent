package rds

import (
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

func TestExtractConfigValues(t *testing.T) {
	proposed := &agent.ProposedConfigResponse{
		KnobsOverrides: []string{"work_mem", "random_page_cost"},
		Config: []agent.PGConfigRow{
			{Name: "work_mem", Setting: 16384, Vartype: "integer"},
			{Name: "random_page_cost", Setting: 1.1, Vartype: "real"},
			{Name: "shared_buffers", Setting: 1024, Vartype: "integer"},
		},
	}

	targets, err := extractConfigValues(proposed)
	require.NoError(t, err)
	require.Len(t, targets, 2, "only overridden knobs are applied")
	assert.Equal(t, configInfo{Name: "work_mem", Value: "16384", Vartype: "integer"}, targets[0])
	assert.Equal(t, "1.1", targets[1].Value)
	assert.Equal(t, []string{"work_mem", "random_page_cost"}, getConfigNames(targets))

	t.Run("unknown knob fails the whole batch", func(t *testing.T) {
		_, err := extractConfigValues(&agent.ProposedConfigResponse{
			KnobsOverrides: []string{"work_mem", "nope"},
			Config:         proposed.Config,
		})
		assert.Error(t, err)
	})
}

func TestStateApplyDebounced(t *testing.T) {
	assert.False(t, (&State{}).ApplyDebounced(time.Minute), "fresh state")
	// Debounced on the attempt, not the success.
	assert.True(t, (&State{LastApplyAttempt: time.Now()}).ApplyDebounced(time.Minute))
	assert.False(t, (&State{LastApplyAttempt: time.Now().Add(-2 * time.Minute)}).ApplyDebounced(time.Minute))
}
