package collectorconfig

import (
	"fmt"
	"time"
)

// CollectorKind distinguishes metric vs catalog collectors.
type CollectorKind uint8

const (
	MetricCollectorKind CollectorKind = 1 << iota
	CatalogCollectorKind
)

// Has reports whether k includes the given kind flag.
func (k CollectorKind) Has(kind CollectorKind) bool {
	return k&kind != 0
}

// BaseConfig holds universal fields that apply to every collector.
type BaseConfig struct {
	Enabled         *bool
	IntervalSeconds *int
}

// IsEnabled returns true if the collector is not explicitly disabled.
func (b BaseConfig) IsEnabled() bool {
	if b.Enabled == nil {
		return true
	}
	return *b.Enabled
}

// IntervalOr returns the configured interval or def when unset or zero.
// A non-zero value less than def is an error — users can slow collection
// down but not speed it up beyond the default.
func (b BaseConfig) IntervalOr(def time.Duration) (time.Duration, error) {
	if b.IntervalSeconds == nil {
		return def, nil
	}
	if *b.IntervalSeconds == 0 {
		return def, nil
	}
	configured := time.Duration(*b.IntervalSeconds) * time.Second
	if configured < def {
		return 0, fmt.Errorf(
			"interval_seconds %d is below the minimum of %d",
			*b.IntervalSeconds, int(def.Seconds()),
		)
	}
	return configured, nil
}

// CollectorEntry holds the parsed config for one collector.
type CollectorEntry struct {
	Base  BaseConfig
	Extra any // per-collector typed config (e.g. PgClassConfig). nil for simple collectors.
}

// IsEnabled returns true if the collector is not explicitly disabled.
func (e CollectorEntry) IsEnabled() bool {
	return e.Base.IsEnabled()
}

// IntervalOr returns the configured interval or def when unset or zero.
func (e CollectorEntry) IntervalOr(def time.Duration) (time.Duration, error) {
	return e.Base.IntervalOr(def)
}

// CollectorsConfig maps collector names to their parsed config.
type CollectorsConfig map[string]CollectorEntry
