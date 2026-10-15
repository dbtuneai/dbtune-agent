package queries

// Capabilities reports what the agent can and can't do on the system it
// monitors. A missing capability doesn't stop the agent: it runs on with
// partial data or features, so the gap would otherwise go unnoticed. Checks
// whose failure makes the agent useless belong in the adapter's constructor,
// which fails startup instead.
//
// Each CapabilityChecker owns one area (the Postgres role, a cloud API, ...).
// The collector runs them all, logs what is missing when first seen and when
// it changes, and sends the results to the backend.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	CapabilitiesName = "capabilities"
	// Grants and policies can change while the agent runs; a fix shows up
	// within a minute.
	CapabilitiesInterval = 1 * time.Minute
)

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

type CapabilitiesPayload struct {
	CollectedAt  time.Time    `json:"collected_at"`
	Capabilities []Capability `json:"capabilities"`
}

// CapabilitiesCollector runs the checkers at startup and every
// CapabilitiesInterval. A payload is sent when any capability changes, plus a
// heartbeat every skipUnchangedMultiplier intervals.
func CapabilitiesCollector(logger *logrus.Logger, checkers ...CapabilityChecker) CatalogCollector {
	tracker := newSkipTracker(skipUnchangedMultiplier)
	var prev map[string]Capability

	return CatalogCollector{
		Name:     CapabilitiesName,
		Interval: CapabilitiesInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			collectedAt := time.Now().UTC()
			var caps []Capability
			for _, checker := range checkers {
				checked, err := checker.Check(ctx)
				if err != nil {
					return nil, fmt.Errorf("%s: %s checker: %w", CapabilitiesName, checker.Name, err)
				}
				for _, c := range checked {
					c.Name = checker.Name + "." + c.Name
					caps = append(caps, c)
				}
			}

			cur := make(map[string]Capability, len(caps))
			for _, c := range caps {
				cur[c.Name] = c
			}
			logCapabilityChanges(logger, prev, caps)
			prev = cur

			hashed, err := json.Marshal(caps)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s for hash: %w", CapabilitiesName, err)
			}
			if tracker.shouldSkip(hashed) {
				return nil, nil
			}
			data, err := json.Marshal(&CapabilitiesPayload{CollectedAt: collectedAt, Capabilities: caps})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s: %w", CapabilitiesName, err)
			}
			return &CollectResult{JSON: data}, nil
		},
	}
}

// logCapabilityChanges logs each capability whose status differs from the
// previous tick (prev is nil on the first). An ok capability is only logged
// when it recovers, so a healthy agent logs nothing.
func logCapabilityChanges(logger *logrus.Logger, prev map[string]Capability, cur []Capability) {
	for _, c := range cur {
		before, seen := prev[c.Name]
		if seen && before.Status == c.Status {
			continue
		}
		switch c.Status {
		case CapabilityMissing:
			logger.Warnf("missing capability %s: %s. Fix with: %s", c.Name, c.Impact, c.Fix)
		case CapabilityUnknown:
			logger.Infof("can't check capability %s: %s", c.Name, c.Error)
		case CapabilityOK:
			if seen {
				logger.Infof("capability %s is now available", c.Name)
			}
		}
	}
}
