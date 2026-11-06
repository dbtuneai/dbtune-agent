package tuning

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/config"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ReloadConfigQuery = `
SELECT pg_reload_conf();
`

func ReloadConfig(pgPool *pgxpool.Pool) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), ReloadConfigQuery)
	if err != nil {
		return err
	}
	return nil
}

const AlterSystemQuery = `
ALTER SYSTEM SET %s = %s;
`

func AlterSystem(pgPool *pgxpool.Pool, name string, value string) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), fmt.Sprintf(AlterSystemQuery, name, value))
	if err != nil {
		return err
	}
	return nil
}

const AlterSystemResetQuery = `
ALTER SYSTEM RESET %s;

`

func AlterSystemReset(pgPool *pgxpool.Pool, name string) error {
	_, err := utils.ExecWithPrefix(pgPool, context.Background(), fmt.Sprintf(AlterSystemResetQuery, name))
	if err != nil {
		return err
	}
	return nil
}

const Select1Query = `
SELECT 1;
`

const restartRequiredParamsQuery = `SELECT name FROM pg_settings WHERE name = ANY($1) AND context = 'postmaster'`

// ValidateRestartPolicy queries which of the supplied parameter names would
// require a PostgreSQL restart and enforces the agent's apply policy:
//
//   - if at least one requires a restart but config.IsRestartAllowed is false,
//     a *RestartNotAllowedError is returned;
//   - if at least one requires a restart but KnobApplication=reload, the apply
//     is refused with a plain error to prevent partial application.
//
// The boolean result reports whether any of the parameters require a restart
// (used by callers to decide whether to perform one). On a policy violation
// the boolean is still true so the caller can log accurately if it wishes.
func ValidateRestartPolicy(
	pgPool *pgxpool.Pool,
	ctx context.Context,
	parameterNames []string,
	knobApp KnobApplication,
) (bool, error) {
	restartRequired, err := RestartRequiredParams(pgPool, ctx, parameterNames)
	if err != nil {
		return false, fmt.Errorf("failed to validate which parameters require restart: %w", err)
	}
	if len(restartRequired) == 0 {
		return false, nil
	}
	if !config.IsRestartAllowed() {
		return true, &RestartNotAllowedError{
			Message: fmt.Sprintf("restart is not allowed in the agent, but %d parameter(s) require restart: %v", len(restartRequired), restartRequired),
		}
	}
	if knobApp == KnobApplicationReload {
		return true, fmt.Errorf("refusing to apply: KnobApplication=reload but %d parameter(s) require restart: %v", len(restartRequired), restartRequired)
	}
	return true, nil
}

// RestartRequiredParams returns the subset of the supplied parameter names
// whose value cannot be changed without restarting PostgreSQL (i.e.
// pg_settings.context = 'postmaster').
func RestartRequiredParams(pgPool *pgxpool.Pool, ctx context.Context, parameterNames []string) ([]string, error) {
	if len(parameterNames) == 0 {
		return nil, nil
	}

	rows, err := utils.QueryWithPrefix(pgPool, ctx, restartRequiredParamsQuery, parameterNames)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_settings for restart-required parameters: %w", err)
	}
	defer rows.Close()

	var restartParams []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan restart-required parameter name: %w", err)
		}
		restartParams = append(restartParams, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_settings results: %w", err)
	}
	return restartParams, nil
}

func WaitPostgresReady(pgPool *pgxpool.Pool, ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for PostgreSQL to come back online: %w", ctx.Err())
		case <-time.After(1 * time.Second):
			// Try to execute a simple query
			_, err := utils.ExecWithPrefix(pgPool, ctx, Select1Query)
			if err == nil {
				return nil
			}
		}
	}
}
