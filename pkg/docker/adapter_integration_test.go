//go:build integration

package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/pg"
	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
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

// Integration tests for DockerContainerAdapter.ApplyConfig. They exercise the
// full apply pipeline against a real PostgreSQL container, including the
// restart codepath that issues a Docker restart on the running container.

const dockerIntegrationPort = "45401"

var (
	dockerPGContainer testcontainers.Container
	dockerContainerID string
	dockerConnStr     string
	dockerClient      *client.Client
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
				HostConfigModifier: func(hc *dockercontainer.HostConfig) {
					hc.PortBindings = nat.PortMap{
						"5432/tcp": []nat.PortBinding{
							{HostIP: "127.0.0.1", HostPort: dockerIntegrationPort},
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
	dockerPGContainer = ctr
	dockerContainerID = ctr.GetContainerID()

	dockerConnStr, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get connection string: %v\n", err)
		_ = ctr.Terminate(ctx)
		os.Exit(1)
	}

	dockerClient, err = client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create docker client: %v\n", err)
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
	pool, err := pgxpool.New(context.Background(), dockerConnStr)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// autoConfSetting returns the value stored in postgresql.auto.conf for a
// parameter, or "" if no entry exists. Reads pg_file_settings, which reflects
// the file on disk without requiring a reload.
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

// livePGSetting returns the currently-applied value of a parameter (i.e.
// the in-memory pg_settings value), useful to confirm a reload took effect.
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

func newAdapter(t *testing.T, pool *pgxpool.Pool) *DockerContainerAdapter {
	t.Helper()
	adapter := &DockerContainerAdapter{
		Config:       Config{ContainerName: dockerContainerID},
		dockerClient: dockerClient,
		PGDriver:     pool,
	}
	adapter.WithLogger(logrus.New())
	return adapter
}

// TestIntegration_Docker_ApplyConfig_Reload_NonRestartParam covers the happy
// path for a parameter that does not require a restart. Validation should
// classify it as reload-only, ALTER SYSTEM is issued, and pg_reload_conf
// makes the new value the live setting.
func TestIntegration_Docker_ApplyConfig_Reload_NonRestartParam(t *testing.T) {
	setAllowRestart(t, false)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "work_mem") })

	adapter := newAdapter(t, pool)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "work_mem", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"work_mem"},
		KnobApplication: agent.KnobApplicationReload,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))

	assert.Equal(t, "8192", autoConfSetting(t, pool, "work_mem"),
		"ALTER SYSTEM should have written work_mem to postgresql.auto.conf")
	// pg_reload_conf() only signals the postmaster; new values become visible
	// via pg_settings asynchronously, so poll instead of asserting once.
	require.Eventually(t, func() bool {
		return livePGSetting(t, pool, "work_mem") == "8192"
	}, 3*time.Second, 20*time.Millisecond,
		"reload should have made work_mem the live value")
}

// TestIntegration_Docker_ApplyConfig_Restart_Allowed covers the restart
// codepath end-to-end: a postmaster-context parameter is altered, the docker
// container is actually restarted, and the agent waits for PostgreSQL to come
// back online. The new value should take effect after the restart.
func TestIntegration_Docker_ApplyConfig_Restart_Allowed(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	adapter := newAdapter(t, pool)

	// shared_buffers is stored as an integer in 8kB units. 4096 = 32MB,
	// small enough to start on any postgres:16-alpine.
	const expected8kBUnits = "4096"

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(4096), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))

	// Container should be running after the restart.
	inspect, err := dockerClient.ContainerInspect(context.Background(), dockerContainerID)
	require.NoError(t, err)
	assert.True(t, inspect.State.Running, "container should be running after restart")

	// Value should be live after the restart took effect.
	assert.Equal(t, expected8kBUnits, livePGSetting(t, pool, "shared_buffers"),
		"shared_buffers should reflect the new value after restart")
}

// TestIntegration_Docker_ApplyConfig_Restart_NotAllowed locks in the fix on
// this branch: when a restart-required parameter is requested but
// postgresql.allow_restart=false, we must bail before mutating
// postgresql.auto.conf, so the system is never half-applied.
func TestIntegration_Docker_ApplyConfig_Restart_NotAllowed(t *testing.T) {
	setAllowRestart(t, false)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	before := autoConfSetting(t, pool, "shared_buffers")
	require.Empty(t, before, "test precondition: shared_buffers must be reset before run")

	adapter := newAdapter(t, pool)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	err := adapter.ApplyConfig(context.Background(), proposed)

	var notAllowed *agent.RestartNotAllowedError
	assert.ErrorAs(t, err, &notAllowed, "expected RestartNotAllowedError, got %v", err)

	assert.Empty(t, autoConfSetting(t, pool, "shared_buffers"),
		"no ALTER SYSTEM should have been issued when restart is not allowed")
}

// TestIntegration_Docker_ApplyConfig_RestartParam_WithReloadIntent guards the
// invariant that a reload-only intent cannot be used to apply a parameter
// that actually requires a restart. The CNPG fix that drove this branch
// (refuse partial application) is enforced for docker too via the shared
// pg.ValidateRestartPolicy helper.
func TestIntegration_Docker_ApplyConfig_RestartParam_WithReloadIntent(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers") })

	adapter := newAdapter(t, pool)

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
		"with allow_restart=true, the error should be a refusal due to reload intent, not RestartNotAllowedError")
	assert.Contains(t, err.Error(), "refusing to apply")
	assert.Contains(t, err.Error(), "shared_buffers")

	assert.Empty(t, autoConfSetting(t, pool, "shared_buffers"),
		"no ALTER SYSTEM should have been issued when reload-intent collides with a restart-required param")
}

// TestIntegration_Docker_ApplyConfig_MixedParams_Restart asserts that when a
// batch contains both restart-required and reload-only params and the apply
// mode is "restart", all parameters land in postgresql.auto.conf and the
// container is restarted exactly once.
func TestIntegration_Docker_ApplyConfig_MixedParams_Restart(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "shared_buffers", "work_mem") })

	adapter := newAdapter(t, pool)

	startedAt := containerStartedAt(t)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "shared_buffers", Setting: int64(4096), Vartype: "integer"},
			{Name: "work_mem", Setting: int64(16384), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"shared_buffers", "work_mem"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))

	assert.NotEmpty(t, autoConfSetting(t, pool, "shared_buffers"))
	assert.Equal(t, "16384", autoConfSetting(t, pool, "work_mem"))

	// Both values should be live after the restart.
	assert.Equal(t, "4096", livePGSetting(t, pool, "shared_buffers"))
	assert.Equal(t, "16384", livePGSetting(t, pool, "work_mem"))

	assert.True(t, containerStartedAt(t).After(startedAt),
		"container should have restarted (StartedAt should have advanced)")
}

// TestIntegration_Docker_ApplyConfig_ReloadOnlyParams_WithRestartIntent
// asserts that KnobApplication=restart is treated as a hint: when no parameter
// actually needs a restart, the docker container is *not* restarted and we
// fall back to pg_reload_conf. This avoids a needless service blip when the
// backend optimistically asks for a restart.
func TestIntegration_Docker_ApplyConfig_ReloadOnlyParams_WithRestartIntent(t *testing.T) {
	setAllowRestart(t, true)
	pool := newPool(t)
	t.Cleanup(func() { resetAutoConf(t, pool, "work_mem") })

	adapter := newAdapter(t, pool)

	startedAt := containerStartedAt(t)

	proposed := &agent.ProposedConfigResponse{
		Config: []agent.PGConfigRow{
			{Name: "work_mem", Setting: int64(8192), Vartype: "integer"},
		},
		KnobsOverrides:  []string{"work_mem"},
		KnobApplication: agent.KnobApplicationRestart,
	}

	require.NoError(t, adapter.ApplyConfig(context.Background(), proposed))

	assert.Equal(t, "8192", autoConfSetting(t, pool, "work_mem"))
	require.Eventually(t, func() bool {
		return livePGSetting(t, pool, "work_mem") == "8192"
	}, 3*time.Second, 20*time.Millisecond,
		"reload should have made work_mem live without a restart")
	assert.Equal(t, startedAt, containerStartedAt(t),
		"container should not have been restarted when no param requires it")
}

// TestIntegration_Docker_ApplyConfig_ValidateUsesLiveCatalog confirms that
// the restart classification comes from the running PostgreSQL's pg_settings,
// not a hardcoded list in the agent. We assert the same classification the
// helper would produce against the same DB.
func TestIntegration_Docker_ApplyConfig_ValidateUsesLiveCatalog(t *testing.T) {
	pool := newPool(t)

	got, err := pg.RestartRequiredParams(pool, context.Background(),
		[]string{"shared_buffers", "work_mem", "max_connections"})
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"shared_buffers", "max_connections"}, got,
		"sanity-check: shared_buffers and max_connections are postmaster, work_mem is user")
}

func containerStartedAt(t *testing.T) time.Time {
	t.Helper()
	inspect, err := dockerClient.ContainerInspect(context.Background(), dockerContainerID)
	require.NoError(t, err)
	ts, err := time.Parse(time.RFC3339Nano, inspect.State.StartedAt)
	require.NoError(t, err)
	return ts
}
