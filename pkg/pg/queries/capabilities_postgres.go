package queries

// The Postgres capability checker: which privilege-dependent data the agent's
// database role can see. Missing privileges don't make queries fail: Postgres
// silently hides other roles' rows or columns.
//
// https://www.postgresql.org/docs/current/predefined-roles.html

import (
	"context"
	"fmt"

	"github.com/dbtuneai/agent/pkg/internal/pgxutil"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const PostgresCapabilityCheckerName = "postgres"

// PostgresRolePrivileges is what the agent's role has. pg_has_role is true
// for a superuser, so every privilege is true for one.
type PostgresRolePrivileges struct {
	Role string `db:"role"`
	// pg_read_all_stats and pg_read_all_settings are granted through pg_monitor.
	ReadAllStats    bool `db:"pg_read_all_stats"`
	ReadAllSettings bool `db:"pg_read_all_settings"`
	// ReadAllData is nil before PostgreSQL 14, which has no pg_read_all_data.
	ReadAllData *bool `db:"pg_read_all_data"`
}

// pg_has_role errors on a role that doesn't exist, so pg_read_all_data is
// looked up by OID: no row, and so NULL, before PostgreSQL 14.
const postgresRolePrivilegesQuery = `SELECT
	current_user AS role,
	pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AS pg_read_all_stats,
	pg_has_role(current_user, 'pg_read_all_settings', 'USAGE') AS pg_read_all_settings,
	(SELECT pg_has_role(current_user, oid, 'USAGE') FROM pg_roles WHERE rolname = 'pg_read_all_data') AS pg_read_all_data`

// postgresCapability is a capability the agent needs from its role and what is
// lost without it. Add a PostgresRolePrivileges field and a row here to check
// another.
type postgresCapability struct {
	name string
	// has reports whether the role has it, or nil when it can't be checked
	// (e.g. a predefined role this Postgres version doesn't have); unknown
	// then says why.
	has     func(PostgresRolePrivileges) *bool
	unknown string
	impact  string
	// fix is a statement with one %s for the quoted role name.
	fix string
}

var postgresCapabilities = []postgresCapability{
	{
		name:   "pg_read_all_stats",
		has:    func(p PostgresRolePrivileges) *bool { return &p.ReadAllStats },
		impact: "other roles' activity is hidden, so query runtimes, connections, wait events and vacuum progress only cover the agent's own role",
		fix:    "GRANT pg_monitor TO %s;",
	},
	{
		name:   "pg_read_all_settings",
		has:    func(p PostgresRolePrivileges) *bool { return &p.ReadAllSettings },
		impact: "superuser-only settings such as shared_preload_libraries are hidden",
		fix:    "GRANT pg_monitor TO %s;",
	},
	{
		name:    "pg_read_all_data",
		has:     func(p PostgresRolePrivileges) *bool { return p.ReadAllData },
		unknown: "pg_read_all_data needs PostgreSQL 14 or later",
		impact:  "dbtune can't read column statistics from pg_stats",
		fix:     "GRANT pg_read_all_data TO %s;",
	},
}

// PostgresCapabilityChecker checks the agent's database role.
func PostgresCapabilityChecker(pool *pgxpool.Pool, prepareCtx PrepareCtx) CapabilityChecker {
	scanner := pgxutil.NewScanner[PostgresRolePrivileges]()
	return CapabilityChecker{
		Name: PostgresCapabilityCheckerName,
		Check: func(ctx context.Context) ([]Capability, error) {
			ctx, err := prepareCtx(ctx)
			if err != nil {
				return nil, err
			}
			rows, err := CollectView(func() (pgx.Rows, error) {
				return utils.QueryWithPrefix(pool, ctx, postgresRolePrivilegesQuery)
			}, "role privileges", scanner)
			if err != nil {
				return nil, err
			}
			if len(rows) != 1 {
				return nil, fmt.Errorf("expected 1 row of role privileges, got %d", len(rows))
			}
			return postgresCapabilitiesFor(rows[0]), nil
		},
	}
}

func postgresCapabilitiesFor(p PostgresRolePrivileges) []Capability {
	quoted := pgx.Identifier{p.Role}.Sanitize()
	caps := make([]Capability, len(postgresCapabilities))
	for i, pc := range postgresCapabilities {
		caps[i] = Capability{
			Name:   pc.name,
			Status: CapabilityOK,
			Impact: pc.impact,
			Fix:    fmt.Sprintf(pc.fix, quoted),
		}
		switch has := pc.has(p); {
		case has == nil:
			caps[i].Status = CapabilityUnknown
			caps[i].Error = pc.unknown
			caps[i].Fix = ""
		case !*has:
			caps[i].Status = CapabilityMissing
		}
	}
	return caps
}
