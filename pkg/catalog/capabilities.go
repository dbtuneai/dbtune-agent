package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtuneai/agent/pkg/capabilities"
	"github.com/sirupsen/logrus"
)

const (
	CapabilitiesName = "capabilities"
	// Grants and policies can change while the agent runs; a fix shows up
	// within a minute.
	CapabilitiesInterval = 1 * time.Minute
)

type CapabilitiesPayload struct {
	CollectedAt  time.Time                 `json:"collected_at"`
	Capabilities []capabilities.Capability `json:"capabilities"`
}

// CapabilitiesCollector runs the checkers at startup and every
// CapabilitiesInterval. A payload is sent when any capability changes, plus a
// heartbeat every skipUnchangedMultiplier intervals.
func CapabilitiesCollector(logger *logrus.Logger, checkers ...capabilities.CapabilityChecker) Collector {
	tracker := newSkipTracker(skipUnchangedMultiplier)
	var prev map[string]capabilities.Capability

	return Collector{
		Name:     CapabilitiesName,
		Interval: CapabilitiesInterval,
		Collect: func(ctx context.Context) (*CollectResult, error) {
			collectedAt := time.Now().UTC()
			var caps []capabilities.Capability
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

			cur := make(map[string]capabilities.Capability, len(caps))
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
func logCapabilityChanges(logger *logrus.Logger, prev map[string]capabilities.Capability, cur []capabilities.Capability) {
	for _, c := range cur {
		before, seen := prev[c.Name]
		if seen && before.Status == c.Status {
			continue
		}
		switch c.Status {
		case capabilities.CapabilityMissing:
			logger.Warnf("missing capability %s: %s. Fix with: %s", c.Name, c.Impact, c.Fix)
		case capabilities.CapabilityUnknown:
			logger.Infof("can't check capability %s: %s", c.Name, c.Error)
		case capabilities.CapabilityOK:
			if seen {
				logger.Infof("capability %s is now available", c.Name)
			}
		}
	}
}
