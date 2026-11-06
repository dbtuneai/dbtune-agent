package tuning

import (
	"fmt"
)

// ApplyConfigError is the closed set of errors AgentLooper.ApplyConfig may
// return. The unexported marker keeps the set closed to this package.
// ErrorType() is the wire string the platform dispatches on.
type ApplyConfigError interface {
	error
	ErrorType() string
	isApplyConfigError()
}

type RestartNotAllowedError struct {
	Message string
}

func (e *RestartNotAllowedError) Error() string { return e.Message }

func (e *RestartNotAllowedError) ErrorType() string { return "restart_not_allowed" }

func (*RestartNotAllowedError) isApplyConfigError() {}

// ConfigApplyError is the fallback variant for ApplyConfig failures with no
// more specific classification.
type ConfigApplyError struct {
	Err error
}

func (e *ConfigApplyError) Error() string { return e.Err.Error() }

func (e *ConfigApplyError) Unwrap() error { return e.Err }

func (e *ConfigApplyError) ErrorType() string { return "config_apply_error" }

func (*ConfigApplyError) isApplyConfigError() {}

// DefaultParameterGroupError is returned when the instance has a default
// parameter group attached. AWS prohibits modifying default.* groups, so the
// agent refuses to apply rather than failing mid-way. The platform reads this
// typed error to surface guidance to the user.
type DefaultParameterGroupError struct {
	ParameterGroupName string
}

func (e *DefaultParameterGroupError) Error() string {
	return fmt.Sprintf("cannot apply config: instance uses default parameter group %q which AWS does not allow modifying", e.ParameterGroupName)
}

func (e *DefaultParameterGroupError) ErrorType() string { return "default_parameter_group" }

func (*DefaultParameterGroupError) isApplyConfigError() {}
