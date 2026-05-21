package aiven

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// TestApplyConfig_BailsBeforeAivenCallWhenRestartForbidden locks in the fix
// from this branch: if a knob would require a restart but the agent is not
// allowed to restart, ApplyConfig must return a RestartNotAllowedError before
// touching the Aiven API. A nil Client is used deliberately, so any call into
// it would panic the test instead of silently passing.
func TestApplyConfig_BailsBeforeAivenCallWhenRestartForbidden(t *testing.T) {
	viper.Reset()
	viper.Set("postgresql.allow_restart", false)
	t.Cleanup(viper.Reset)

	adapter := &AivenPostgreSQLAdapter{
		Client: nil, // any call into the Aiven client would panic
	}
	adapter.WithLogger(logrus.New())

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers_percentage", Setting: 25.0, Vartype: "real"},
		},
		KnobsOverrides:  []string{"shared_buffers_percentage"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	err := adapter.ApplyConfig(context.Background(), proposed)

	var notAllowed *agent.RestartNotAllowedError
	assert.ErrorAs(t, err, &notAllowed, "expected RestartNotAllowedError, got %v", err)
}

// TestApplyConfig_ProceedsWhenRestartAllowed sanity-checks that the early
// bail does not fire when restarts are permitted: a nil Client now produces
// a nil-pointer panic, so we recover and assert that the Aiven call was
// reached.
func TestApplyConfig_ProceedsWhenRestartAllowed(t *testing.T) {
	viper.Reset()
	viper.Set("postgresql.allow_restart", true)
	t.Cleanup(viper.Reset)

	adapter := &AivenPostgreSQLAdapter{
		Client: nil,
	}
	adapter.WithLogger(logrus.New())

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers_percentage", Setting: 25.0, Vartype: "real"},
		},
		KnobsOverrides:  []string{"shared_buffers_percentage"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	defer func() {
		r := recover()
		assert.NotNil(t, r, "expected to reach the Aiven Client call (and panic on nil), but ApplyConfig returned cleanly")
	}()

	err := adapter.ApplyConfig(context.Background(), proposed)
	var notAllowed *agent.RestartNotAllowedError
	assert.False(t, errors.As(err, &notAllowed), "should not bail with RestartNotAllowedError when restarts are allowed")
}
