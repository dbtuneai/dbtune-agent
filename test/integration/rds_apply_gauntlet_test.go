//go:build rds_integration

// End-to-end "gauntlet" test for RDSAdapter.ApplyConfig.
//
// This test drives the agent's full apply path against a real RDS or Aurora
// instance with every parameter the platform's tuning space declares for the
// chosen provider, in two scenarios per run:
//
//   1. Attach a `default.*` parameter group → expect refusal
//      (`*agent.DefaultParameterGroupError`) for every batch.
//   2. Attach a custom parameter group       → expect success for every batch.
//
// The test itself performs the PG swap via `rds:ModifyDBInstance` and waits
// for the parameter-apply status to settle before constructing the adapter,
// so you do not need to flip parameter groups by hand between runs.
//
// The static-parameter batch issues a real reboot of the target instance and
// is opt-in via `DBTUNE_TEST_ALLOW_REBOOT=true`.
//
// ─── ONE-TIME AWS SETUP (resources expected to already exist) ────────────────
//
//   * A DB subnet group reachable from where you run the tests.
//   * A custom DB parameter group of family `postgres16`            (for RDS).
//   * A custom DB parameter group of family `aurora-postgresql16`   (for Aurora).
//   * A plain RDS Postgres 16 instance attached to either group (the test
//     swaps it between `default.postgres16` and your custom group).
//   * An Aurora PostgreSQL 16 cluster with at least one instance attached
//     (writer; optionally also a reader). Same swap mechanism applies to
//     each instance's parameter group.
//
// The defaults `DBTUNE_TEST_CUSTOM_PG` falls back to assume the naming
// convention used in this repo's example setup:
//
//   * RDS:    `dbtune-test-custom-pg16`
//   * Aurora: `dbtune-test-custom-aurora-pg16`
//
// Override `DBTUNE_TEST_CUSTOM_PG` if your custom group has a different name.
//
// Caller-side prerequisites for each run:
//
//   * AWS credentials available to the default chain (e.g. `aws sso login`).
//   * Network reachability to the target DB (public IP, VPN, peering, etc.).
//   * A Postgres role on the target DB that can connect via the URL below.
//
// ─── HOW TO RUN ──────────────────────────────────────────────────────────────
//
// Required env:
//
//   export DBT_AWS_REGION=<region>
//   export DBT_RDS_DATABASE_IDENTIFIER=<db-instance-identifier>
//   export DBT_POSTGRESQL_CONNECTION_URL='postgres://user:pass@<host>:5432/postgres?sslmode=require'
//   export DBTUNE_TEST_PROVIDER=<rds | aurora>
//
// Optional env:
//
//   export DBTUNE_TEST_CUSTOM_PG=<name>      # override default custom PG per provider
//   export DBTUNE_TEST_ALLOW_REBOOT=true     # required to exercise the static
//                                            # batch on the custom-PG scenario
//                                            # (triggers a real reboot)
//
// Run:
//
//   go test -tags=rds_integration -v -timeout=30m -run TestApplyConfig_FullScenario ./test/integration/...
//
// The test will, for the chosen identifier:
//
//   1. ModifyDBInstance → attach `default.<engine>`
//   2. Wait for parameter-apply status `in-sync` or `pending-reboot`
//   3. Build adapter, run dynamic + static batches → expect refusal
//   4. ModifyDBInstance → attach the custom PG
//   5. Wait for parameter-apply status to settle
//   6. Build adapter, run dynamic batch → expect success
//   7. If DBTUNE_TEST_ALLOW_REBOOT=true and the provider has static knobs,
//      run static batch → expect success (instance reboots once).

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type knob struct {
	name    string
	setting any
	unit    any
	vartype string
	static  bool
}

func (k knob) toRow() agent.PGConfigRow {
	ctx := "user"
	if k.static {
		ctx = "postmaster"
	}
	return agent.PGConfigRow{
		Name:    k.name,
		Setting: k.setting,
		Unit:    k.unit,
		Vartype: k.vartype,
		Context: ctx,
	}
}

// pkg-tuning-platform DatabaseProvider.RDS — tunables + heuristics + defaults.
var rdsKnobs = []knob{
	{name: "bgwriter_lru_maxpages", setting: 200, vartype: "integer"},
	{name: "effective_io_concurrency", setting: 200, vartype: "integer"},
	{name: "max_parallel_workers", setting: 4, vartype: "integer"},
	{name: "max_parallel_workers_per_gather", setting: 2, vartype: "integer"},
	{name: "max_wal_size", setting: 1024, unit: "MB", vartype: "integer"},
	{name: "random_page_cost", setting: 1.1, vartype: "real"},
	{name: "shared_buffers", setting: 32768, unit: "8kB", vartype: "integer", static: true},
	{name: "work_mem", setting: 8192, unit: "kB", vartype: "integer"},
	{name: "bgwriter_delay", setting: 200, unit: "ms", vartype: "integer"},
	{name: "min_wal_size", setting: 128, unit: "MB", vartype: "integer"},
	{name: "seq_page_cost", setting: 1.0, vartype: "real"},
	{name: "max_worker_processes", setting: 8, vartype: "integer", static: true},
	{name: "effective_cache_size", setting: 16384, unit: "8kB", vartype: "integer"},
	{name: "maintenance_work_mem", setting: 65536, unit: "kB", vartype: "integer"},
	{name: "default_statistics_target", setting: 100, vartype: "integer"},
	{name: "max_parallel_maintenance_workers", setting: 2, vartype: "integer"},
	{name: "checkpoint_completion_target", setting: 0.9, vartype: "real"},
	{name: "wal_buffers", setting: 512, unit: "8kB", vartype: "integer", static: true},
	{name: "huge_pages", setting: "off", vartype: "enum", static: true},
}

// pkg-tuning-platform DatabaseProvider.AURORA — all dynamic on PG16.
var auroraKnobs = []knob{
	{name: "max_parallel_workers", setting: 4, vartype: "integer"},
	{name: "max_parallel_workers_per_gather", setting: 2, vartype: "integer"},
	{name: "random_page_cost", setting: 1.1, vartype: "real"},
	{name: "work_mem", setting: 8192, unit: "kB", vartype: "integer"},
	{name: "seq_page_cost", setting: 1.0, vartype: "real"},
}

func knobsForProvider(t *testing.T, provider string) []knob {
	t.Helper()
	switch strings.ToLower(provider) {
	case "rds":
		return rdsKnobs
	case "aurora":
		return auroraKnobs
	default:
		t.Fatalf("DBTUNE_TEST_PROVIDER must be 'rds' or 'aurora', got %q", provider)
		return nil
	}
}

func defaultPGFor(provider string) string {
	switch strings.ToLower(provider) {
	case "rds":
		return "default.postgres16"
	case "aurora":
		return "default.aurora-postgresql16"
	}
	return ""
}

func defaultCustomPG(provider string) string {
	switch strings.ToLower(provider) {
	case "rds":
		return "dbtune-test-custom-pg16"
	case "aurora":
		return "dbtune-test-custom-aurora-pg16"
	}
	return ""
}

func splitByMode(knobs []knob) (dynamic, static []knob) {
	for _, k := range knobs {
		if k.static {
			static = append(static, k)
		} else {
			dynamic = append(dynamic, k)
		}
	}
	return
}

func buildProposed(knobs []knob, mode agent.KnobApplication) *agent.ProposedConfigResponse {
	rows := make([]agent.PGConfigRow, 0, len(knobs))
	names := make([]string, 0, len(knobs))
	for _, k := range knobs {
		rows = append(rows, k.toRow())
		names = append(names, k.name)
	}
	return &agent.ProposedConfigResponse{
		Config:          rows,
		KnobsOverrides:  names,
		KnobApplication: mode,
	}
}

func newRDSClient(t *testing.T, region string) *awsrds.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	require.NoError(t, err, "failed to load AWS config from default chain")
	return awsrds.NewFromConfig(cfg)
}

// attachParameterGroup swaps the instance's attached DB parameter group via
// rds:ModifyDBInstance and blocks until the new group reaches a settled
// status (`in-sync` or `pending-reboot`). The instance itself remains
// `available` during a PG swap — no reboot is triggered here.
func attachParameterGroup(t *testing.T, c *awsrds.Client, identifier, pgName string) {
	t.Helper()
	t.Logf("attach %q → %q", pgName, identifier)
	_, err := c.ModifyDBInstance(context.Background(), &awsrds.ModifyDBInstanceInput{
		DBInstanceIdentifier: awsv2.String(identifier),
		DBParameterGroupName: awsv2.String(pgName),
		ApplyImmediately:     awsv2.Bool(true),
	})
	require.NoError(t, err, "ModifyDBInstance failed")

	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := c.DescribeDBInstances(context.Background(), &awsrds.DescribeDBInstancesInput{
			DBInstanceIdentifier: awsv2.String(identifier),
		})
		require.NoError(t, err)
		if len(out.DBInstances) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}
		for _, pg := range out.DBInstances[0].DBParameterGroups {
			if awsv2.ToString(pg.DBParameterGroupName) != pgName {
				continue
			}
			status := awsv2.ToString(pg.ParameterApplyStatus)
			if status == "in-sync" || status == "pending-reboot" {
				t.Logf("attached %q reached status %q", pgName, status)
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("timed out waiting for %q to settle on %q", pgName, identifier)
}

func TestApplyConfig_FullScenario(t *testing.T) {
	region := requireEnv(t, "DBT_AWS_REGION")
	identifier := requireEnv(t, "DBT_RDS_DATABASE_IDENTIFIER")
	provider := requireEnv(t, "DBTUNE_TEST_PROVIDER")
	requireEnv(t, "DBT_POSTGRESQL_CONNECTION_URL")

	customPG := envOrDefault("DBTUNE_TEST_CUSTOM_PG", defaultCustomPG(provider))
	defaultPG := defaultPGFor(provider)
	require.NotEmpty(t, defaultPG, "unknown provider %q", provider)
	require.NotEmpty(t, customPG, "no default custom PG name for provider %q; set DBTUNE_TEST_CUSTOM_PG", provider)

	allowReboot := strings.EqualFold(envOrDefault("DBTUNE_TEST_ALLOW_REBOOT", ""), "true")

	knobs := knobsForProvider(t, provider)
	dyn, sta := splitByMode(knobs)
	rdsClient := newRDSClient(t, region)

	scenarios := []struct {
		name        string
		targetPG    string
		wantRefused bool
	}{
		{name: "refusal_on_default_PG", targetPG: defaultPG, wantRefused: true},
		{name: "success_on_custom_PG", targetPG: customPG, wantRefused: false},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			attachParameterGroup(t, rdsClient, identifier, sc.targetPG)

			adapter := newAdapter(t)
			viper.Set("postgresql.allow_restart", allowReboot)

			pgName := adapter.State.DBInfo.ParameterGroupName
			require.Equal(t, sc.targetPG, pgName, "adapter must observe the attached PG")
			t.Logf("provider=%s identifier=%s attached PG=%q (refusal expected=%v) allow_reboot=%v",
				provider, identifier, pgName, sc.wantRefused, allowReboot)

			t.Run("dynamic_batch_reload", func(t *testing.T) {
				if len(dyn) == 0 {
					t.Skip("no dynamic knobs for this provider")
				}
				adapter.State.LastAppliedConfig = time.Time{}
				err := adapter.ApplyConfig(context.Background(),
					buildProposed(dyn, agent.KnobApplicationReload))
				assertApplyOutcome(t, err, pgName, sc.wantRefused)
			})

			t.Run("static_batch_restart", func(t *testing.T) {
				if len(sta) == 0 {
					t.Skip("no static knobs for this provider")
				}
				if !sc.wantRefused && !allowReboot {
					t.Skip("static batch on custom PG reboots the instance; set DBTUNE_TEST_ALLOW_REBOOT=true to run")
				}
				adapter.State.LastAppliedConfig = time.Time{}
				err := adapter.ApplyConfig(context.Background(),
					buildProposed(sta, agent.KnobApplicationRestart))
				assertApplyOutcome(t, err, pgName, sc.wantRefused)
			})
		})
	}
}

func assertApplyOutcome(t *testing.T, err agent.ApplyConfigError, pgName string, wantRefused bool) {
	t.Helper()
	if wantRefused {
		var typed *agent.DefaultParameterGroupError
		require.Error(t, err, "default PG must trigger refusal")
		require.True(t, errors.As(err, &typed),
			"expected *agent.DefaultParameterGroupError, got %T: %v", err, err)
		assert.Equal(t, pgName, typed.ParameterGroupName)
		assert.Equal(t, "default_parameter_group", err.ErrorType())
		return
	}
	require.NoError(t, err, "apply against custom PG %q must succeed", pgName)
}
