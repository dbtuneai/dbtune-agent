package collectorconfig

import "time"

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

// IntervalOr returns the configured interval clamped to at least def.
// Users can slow down collection but not speed it up beyond the default.
func (b BaseConfig) IntervalOr(def time.Duration) time.Duration {
	if b.IntervalSeconds == nil {
		return def
	}
	configured := time.Duration(*b.IntervalSeconds) * time.Second
	if configured < def {
		return def
	}
	return configured
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

// IntervalOr returns the configured interval clamped to at least def.
func (e CollectorEntry) IntervalOr(def time.Duration) time.Duration {
	return e.Base.IntervalOr(def)
}

// CollectorsConfig maps collector names to their parsed config.
type CollectorsConfig map[string]CollectorEntry
