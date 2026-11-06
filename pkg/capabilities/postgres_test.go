package capabilities

import (
	"strings"
	"testing"
)

func TestPostgresCapabilitiesFor(t *testing.T) {
	caps := postgresCapabilitiesFor(PostgresRolePrivileges{Role: `odd"role`, ReadAllSettings: true})
	got := strings.Join(capabilityNames(caps), " ")
	if want := "pg_read_all_stats=missing pg_read_all_settings=ok"; got != want {
		t.Errorf("capabilities = %s, want %s", got, want)
	}
	if want := `GRANT pg_monitor TO "odd""role";`; caps[0].Fix != want {
		t.Errorf("fix = %q, want %q", caps[0].Fix, want)
	}
	if caps[0].Impact == "" {
		t.Error("expected an impact")
	}
}

// Every capability's fix must take exactly the role name.
func TestPostgresCapabilities_Fix(t *testing.T) {
	for _, pc := range postgresCapabilities {
		if strings.Count(pc.fix, "%") != 1 || strings.Count(pc.fix, "%s") != 1 {
			t.Errorf("%s: fix %q must have exactly one %%s and no other verbs", pc.name, pc.fix)
		}
	}
}

func capabilityNames(caps []Capability) []string {
	names := make([]string, len(caps))
	for i, c := range caps {
		names[i] = c.Name + "=" + string(c.Status)
	}
	return names
}
