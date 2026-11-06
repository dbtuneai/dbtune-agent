// Package capabilities reports what the agent can and can't do on the system
// it monitors. A missing capability doesn't stop the agent: it runs on with
// partial data or features, so the gap would otherwise go unnoticed. Checks
// whose failure makes the agent useless belong in the adapter's constructor,
// which fails startup instead.
//
// Each CapabilityChecker owns one area (the Postgres role, a cloud API, ...).
// The capabilities collector in pkg/catalog runs them all, logs what is
// missing when first seen and when it changes, and sends the results to the
// backend.
package capabilities

import "context"

type CapabilityStatus string

const (
	CapabilityOK      CapabilityStatus = "ok"
	CapabilityMissing CapabilityStatus = "missing"
	// CapabilityUnknown: the checker can't tell, e.g. a write permission that
	// can't be tested without writing. Error says why.
	CapabilityUnknown CapabilityStatus = "unknown"
)

// Capability is one checked capability. Name is unique within its checker;
// the collector prefixes it with the checker's name. Use the checked
// system's own vocabulary so it's recognisable: a Postgres role name
// ("pg_read_all_stats"), an IAM action ("rds:DescribeDBClusters"), ...
type Capability struct {
	Name   string           `json:"name"`
	Status CapabilityStatus `json:"status"`
	// Impact is what the agent loses without it, Fix how to grant it. Both
	// are shown to the user as is.
	Impact string `json:"impact,omitempty"`
	Fix    string `json:"fix,omitempty"`
	Error  string `json:"error,omitempty"`
}

// CapabilityChecker checks one area. Name is a short lowercase namespace
// ("postgres", "aws", "kubernetes"). Check returns every capability it
// knows about, ok ones included, so the backend can tell a fixed capability
// from one that was never checked. An error fails the whole tick, like any
// collector error; a capability that can't be checked is CapabilityUnknown.
type CapabilityChecker struct {
	Name  string
	Check func(ctx context.Context) ([]Capability, error)
}
