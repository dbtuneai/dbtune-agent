//go:build integration

package pgprem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Integration tests for DefaultPostgreSQLAdapter.ApplyConfig.
//
// pgprem talks to a *local* PostgreSQL whose lifecycle it controls through
// systemctl. We can't realistically point systemctl at a testcontainer, so
// the actual restart path is exercised in two halves:
//
//   - the pre-restart validation, ALTER SYSTEM, and policy enforcement run
//     against a real PostgreSQL (the testcontainer);
//   - the restart-attempt itself is covered by a separate test that runs the
//     real systemctl path against a bogus service name and asserts it fails
//     with the expected error (skipped on platforms without systemctl).

const pgpremIntegrationPort = "45402"

var (
	pgpremContainer testcontainers.Container
	pgpremConnStr   string
)

type bufLogger struct{ buf *bytes.Buffer }

func (l *bufLogger) Printf(format string, v ...any) {
	fmt.Fprintf(l.buf, format+"\n", v...)
}

func TestMain(m *testing.M) {
	var logBuf bytes.Buffer
	tclog.SetDefault(&bufLogger{buf: &logBuf})
	log.SetOutput(&logBuf)

	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				HostConfigModifier: func(hc *container.HostConfig) {
					hc.PortBindings = nat.PortMap{
						"5432/tcp": []nat.PortBinding{
							{HostIP: "127.0.0.1", HostPort: pgpremIntegrationPort},
						},
					}
				},
			},
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres: %v\n", err)
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
		os.Exit(1)
	}
	pgpremContainer = ctr
	pgpremConnStr, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get connection string: %v\n", err)
		_ = ctr.Terminate(ctx)
		os.Exit(1)
	}

	code := m.Run()
	_ = ctr.Terminate(ctx)
	if code != 0 {
		fmt.Fprintf(os.Stderr, "--- testcontainers log ---\n%s", logBuf.String())
	}
	os.Exit(code)
}

func setAllowRestart(t *testing.T, allow bool) {
	t.Helper()
	viper.Reset()
	viper.Set("postgresql.allow_restart", allow)
	t.Cleanup(viper.Reset)
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), pgpremConnStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func autoConfSetting(t *testing.T, pool *pgxpool.Pool, name string) string {
	t.Helper()
	var setting string
	row := pool.QueryRow(context.Background(),
		`SELECT setting FROM pg_file_settings
		 WHERE name = $1 AND sourcefile LIKE '%postgresql.auto.conf'
		 ORDER BY seqno DESC LIMIT 1`, name)
	if err := row.Scan(&setting); err != nil {
		return ""
	}
	return setting
}

func livePGSetting(t *testing.T, pool *pgxpool.Pool, name string) string {
	t.Helper()
	var setting string
	require.NoError(t,
		pool.QueryRow(context.Background(),
			`SELECT setting FROM pg_settings WHERE name = $1`, name).Scan(&setting),
	)
	return setting
}

func resetAutoConf(t *testing.T, pool *pgxpool.Pool, names ...string) {
	t.Helper()
	ctx := context.Background()
	for _, n := range names {
		_, err := pool.Exec(ctx, fmt.Sprintf("ALTER SYSTEM RESET %s", n))
		require.NoError(t, err)
	}
	_, err := pool.Exec(ctx, "SELECT pg_reload_conf()")
	require.NoError(t, err)
}

func newAdapter(t *testing.T, pool *pgxpool.Pool, serviceName string) *DefaultPostgreSQLAdapter {
	t.Helper()
	adapter := &DefaultPostgreSQLAdapter{
		pgDriver: pool,
		pgConfig: pg.Config{
			ConnectionURL: pgpremConnStr,
			ServiceName:   serviceName,
		},
	}
	adapter.WithLogger(logrus.New())
	return adapter
}

// TestIntegration_Pgprem_ApplyConfig_Reload_NonRestartParam confirms the
// reload-only path against real PostgreSQL: ALTER SYSTEM is issued and
// pg_reload_conf makes the new value live.
func TestIntegration_Pgprem_ApplyConfig_Reload_NonRestartParam(t *testing.T) {
	setAllowRestart(t, false)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "work_mem") })

	adapter := newAdapter(t, pool, "" /* no service: not needed for reload-only */)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "work_mem", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"work_mem"},
		KnobApplication: agent.KnobApplicationReload,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))

	assert.Equal(t, "8192", autoConfSetting(t, pool, "work_mem"))
	require.Eventually(t, func() bool {
		return livePGSetting(t, pool, "work_mem") == "8192"
	}, 3*time.Second, 20*time.Millisecond)
}

// TestIntegration_Pgprem_ApplyConfig_Restart_NotAllowed locks in the
// pre-mutation bail when a restart is requested but allow_restart=false.
func TestIntegration_Pgprem_ApplyConfig_Restart_NotAllowed(t *testing.T) {
	setAllowRestart(t, false)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	adapter := newAdapter(t, pool, "fake.service")

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	err := adapter.ApplyConfig(context.Background(), proposed)

	var notAllowed *agent.RestartNotAllowedError
	assert.ErrorAs(t, err, &notAllowed)
	assert.Empty(t, autoConfSetting(t, pool, "shared_buffers"),
		"no ALTER SYSTEM should have been issued when restart is not allowed")
}

// TestIntegration_Pgprem_ApplyConfig_RestartParam_WithReloadIntent asserts
// that a restart-required parameter cannot be smuggled in under a reload
// intent: the apply is refused before any mutation.
func TestIntegration_Pgprem_ApplyConfig_RestartParam_WithReloadIntent(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	adapter := newAdapter(t, pool, "fake.service")

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers"},
		KnobApplication: agent.KnobApplicationReload,
	}

	err := adapter.ApplyConfig(context.Background(), proposed)
	require.Error(t, err)

	var notAllowed *agent.RestartNotAllowedError
	assert.False(t, errors.As(err, &notAllowed),
		"with allow_restart=true the error must reflect the reload-intent refusal, not RestartNotAllowedError")
	assert.Contains(t, err.Error(), "refusing to apply")

	assert.Empty(t, autoConfSetting(t, pool, "shared_buffers"))
}

// TestIntegration_Pgprem_ApplyConfig_Restart_NoServiceName covers the
// pgprem-specific guard: when a restart is required but no service name is
// configured, the agent refuses to apply (since it would have no way to
// actually realise the change).
func TestIntegration_Pgprem_ApplyConfig_Restart_NoServiceName(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	adapter := newAdapter(t, pool, "" /* unset */)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	err := adapter.ApplyConfig(context.Background(), proposed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service name not configured")

	assert.Empty(t, autoConfSetting(t, pool, "shared_buffers"),
		"no ALTER SYSTEM should have been issued when the agent cannot perform the required restart")
}

// TestIntegration_Pgprem_ApplyConfig_Reload_NoServiceName confirms that
// reload-only applies do *not* trip the service-name guard: the service is
// only needed when a restart will be attempted.
func TestIntegration_Pgprem_ApplyConfig_Reload_NoServiceName(t *testing.T) {
	setAllowRestart(t, false)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "work_mem") })

	adapter := newAdapter(t, pool, "")

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "work_mem", Setting: int64(16384), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"work_mem"},
		KnobApplication: agent.KnobApplicationReload,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))
	// pg_reload_conf is async: poll for the new live value.
	require.Eventually(t, func() bool {
		return livePGSetting(t, pool, "work_mem") == "16384"
	}, 3*time.Second, 20*time.Millisecond)
}

// TestIntegration_Pgprem_ApplyConfig_ReloadOnlyParams_WithRestartIntent
// asserts that KnobApplication=restart is treated as a hint: when no
// parameter actually needs a restart, the systemctl path is not entered,
// even if ServiceName is bogus. The reload-only fallback applies the change.
func TestIntegration_Pgprem_ApplyConfig_ReloadOnlyParams_WithRestartIntent(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "work_mem") })

	// If the systemctl path were entered, this bogus name would cause the
	// command to fail. The test passing proves it was never invoked.
	adapter := newAdapter(t, pool, "dbtune-pgprem-this-must-not-be-touched.service")

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "work_mem", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"work_mem"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))
	require.Eventually(t, func() bool {
		return livePGSetting(t, pool, "work_mem") == "8192"
	}, 3*time.Second, 20*time.Millisecond)
}

// TestIntegration_Pgprem_ApplyConfig_Restart_AttemptsSystemctl exercises the
// actual restart path. We point the adapter at a bogus systemd unit and
// assert that:
//
//   - validation succeeds (shared_buffers is correctly classified as restart);
//   - ALTER SYSTEM mutations land in postgresql.auto.conf;
//   - the systemctl invocation is reached and fails with a propagated error.
//
// Skipped on platforms where systemctl is not on PATH, because we cannot
// observe the restart attempt there without spurious sudo prompts.
func TestIntegration_Pgprem_ApplyConfig_Restart_AttemptsSystemctl(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("systemctl restart path is Linux-only")
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		t.Skip("systemctl not available on PATH")
	}

	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	const bogusService = "dbtune-pgprem-integration-test-nonexistent.service"
	adapter := newAdapter(t, pool, bogusService)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(4096), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	err := adapter.ApplyConfig(context.Background(), proposed)
	require.Error(t, err, "restart against a bogus service must fail")
	assert.Contains(t, err.Error(), "failed to restart PostgreSQL service")

	// Validation+ALTER SYSTEM ran before the restart attempt: the value
	// should be present in postgresql.auto.conf even though the restart
	// itself failed. (This mirrors the production behaviour: by the time
	// we attempt the restart, the file has been mutated.)
	assert.Equal(t, "4096", autoConfSetting(t, pool, "shared_buffers"))
}
