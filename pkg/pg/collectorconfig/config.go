package collectorconfig

import (
	"fmt"
	"time"
)

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

// TypedEntry pairs a BaseConfig with a typed per-collector config.
type TypedEntry[T any] struct {
	Base  BaseConfig
	Extra T
}

// IsEnabled returns true if the collector is not explicitly disabled.
func (e TypedEntry[T]) IsEnabled() bool {
	return e.Base.IsEnabled()
}

// IntervalOr returns the configured interval or def when unset or zero.
func (e TypedEntry[T]) IntervalOr(def time.Duration) (time.Duration, error) {
	return e.Base.IntervalOr(def)
}
