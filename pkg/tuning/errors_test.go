package tuning

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Compile-time check: all variants satisfy ApplyConfigError.
var (
	_ ApplyConfigError = (*ConfigApplyError)(nil)
	_ ApplyConfigError = (*RestartNotAllowedError)(nil)
	_ ApplyConfigError = (*DefaultParameterGroupError)(nil)
)

func TestApplyConfigError_ErrorTypes(t *testing.T) {
	t.Run("ConfigApplyError", func(t *testing.T) {
		inner := errors.New("driver failure")
		e := &ConfigApplyError{Err: inner}
		assert.Equal(t, "config_apply_error", e.ErrorType())
		assert.Equal(t, "driver failure", e.Error())
		assert.ErrorIs(t, e, inner)
	})

	t.Run("RestartNotAllowedError", func(t *testing.T) {
		e := &RestartNotAllowedError{Message: "restart required but allow_restart=false"}
		assert.Equal(t, "restart_not_allowed", e.ErrorType())
		assert.Equal(t, "restart required but allow_restart=false", e.Error())
	})
}
