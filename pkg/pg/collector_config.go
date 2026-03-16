package pg

import (
	"fmt"
	"strings"
	"time"
)

const (
	// MaxDiffLimit is the hard upper bound for pg_stat_statements diff_limit.
	MaxDiffLimit = 500

	// MaxQueryTextLength is the hard upper bound for query text truncation.
	MaxQueryTextLength = 8192
)

// CollectorOverride holds per-collector config overrides.
// All pointer fields: nil = use compiled default.
type CollectorOverride struct {
	Enabled         *bool `mapstructure:"enabled"`
	IntervalSeconds *int  `mapstructure:"interval_seconds"`

	// pg_stat_statements specific
	IncludeQueries     *bool `mapstructure:"include_queries"`
	MaxQueryTextLength *int  `mapstructure:"max_query_text_length"`
	DiffLimit          *int  `mapstructure:"diff_limit"`

	// pg_stats — enables collecting potentially sensitive data values from tables
	// (most_common_vals, histogram_bounds, most_common_elems).
	IncludeTableData *bool `mapstructure:"include_table_data"`

	// pg_stats, pg_class
	BackfillBatchSize *int `mapstructure:"backfill_batch_size"`

	// pg_stat_user_tables, pg_stat_user_indexes, pg_statio_user_indexes
	CategoryLimit *int `mapstructure:"category_limit"`
}

// CollectorsConfig maps collector name → overrides.
type CollectorsConfig map[string]CollectorOverride

// IsEnabled returns true if Enabled is nil or *Enabled is true.
func (o CollectorOverride) IsEnabled() bool {
	if o.Enabled == nil {
		return true
	}
	return *o.Enabled
}

// IntervalOr returns the configured interval clamped to >= def (the compiled default).
// If IntervalSeconds is nil, returns def unchanged.
// Users can only make collection LESS frequent, never more frequent.
func (o CollectorOverride) IntervalOr(def time.Duration) time.Duration {
	if o.IntervalSeconds == nil {
		return def
	}
	configured := time.Duration(*o.IntervalSeconds) * time.Second
	if configured < def {
		return def
	}
	return configured
}

// IntOr returns *ptr if non-nil, else def.
func IntOr(ptr *int, def int) int {
	if ptr == nil {
		return def
	}
	return *ptr
}

// BoolOr returns *ptr if non-nil, else def.
func BoolOr(ptr *bool, def bool) bool {
	if ptr == nil {
		return def
	}
	return *ptr
}

// knownCollectorFields maps collector name → set of valid extra fields
// (beyond the universal "enabled" and "interval_seconds").
var knownCollectorFields = map[string][]string{
	"ddl":                           {},
	"pg_stat_statements":            {"include_queries", "diff_limit"},
	"pg_stats":                      {"backfill_batch_size", "include_table_data"},
	"pg_class":                      {"backfill_batch_size"},
	"pg_stat_user_tables":           {"category_limit"},
	"pg_stat_user_indexes":          {"category_limit"},
	"pg_statio_user_indexes":        {"category_limit"},
	"pg_stat_activity":              {},
	"pg_stat_database":              {},
	"pg_stat_database_conflicts":    {},
	"pg_stat_archiver":              {},
	"pg_stat_bgwriter":              {},
	"pg_stat_checkpointer":          {},
	"pg_stat_wal":                   {},
	"pg_stat_io":                    {},
	"pg_stat_replication":           {},
	"pg_stat_replication_slots":     {},
	"pg_stat_slru":                  {},
	"pg_statio_user_tables":         {},
	"pg_stat_user_functions":        {},
	"pg_locks":                      {},
	"pg_stat_progress_vacuum":       {},
	"pg_stat_progress_analyze":      {},
	"pg_stat_progress_create_index": {},
	"pg_prepared_xacts":             {},
	"pg_replication_slots":          {},
	"pg_index":                      {},
	"pg_stat_wal_receiver":          {},
	"pg_stat_recovery_prefetch":     {},
	"pg_stat_subscription":          {},
	"pg_stat_subscription_stats":    {},
	"wait_events":                   {},
	"autovacuum_count":              {},
	"server_uptime":                 {},
	"database_connections":          {},
	"system_db_size":                {},
	"database_transactions":         {},
}

// KnownCollectorNames returns all known collector names.
func KnownCollectorNames() []string {
	names := make([]string, 0, len(knownCollectorFields))
	for name := range knownCollectorFields {
		names = append(names, name)
	}
	return names
}

// validateCollectorsConfig checks for unknown collector names and invalid values.
func validateCollectorsConfig(cfg CollectorsConfig) error {
	var errs []string
	for name, override := range cfg {
		if _, ok := knownCollectorFields[name]; !ok {
			errs = append(errs, fmt.Sprintf("unknown collector %q", name))
			continue
		}
		if override.IntervalSeconds != nil && *override.IntervalSeconds <= 0 {
			errs = append(errs, fmt.Sprintf("collector %q: interval_seconds must be > 0", name))
		}
		if override.BackfillBatchSize != nil && *override.BackfillBatchSize < 0 {
			errs = append(errs, fmt.Sprintf("collector %q: backfill_batch_size must be >= 0", name))
		}
		if override.CategoryLimit != nil && *override.CategoryLimit < 0 {
			errs = append(errs, fmt.Sprintf("collector %q: category_limit must be >= 0", name))
		}
		if override.DiffLimit != nil {
			if *override.DiffLimit < 0 {
				errs = append(errs, fmt.Sprintf("collector %q: diff_limit must be >= 0", name))
			} else if *override.DiffLimit > MaxDiffLimit {
				errs = append(errs, fmt.Sprintf("collector %q: diff_limit must be <= %d", name, MaxDiffLimit))
			}
		}
		if override.MaxQueryTextLength != nil {
			if *override.MaxQueryTextLength < 0 {
				errs = append(errs, fmt.Sprintf("collector %q: max_query_text_length must be >= 0", name))
			} else if *override.MaxQueryTextLength > MaxQueryTextLength {
				errs = append(errs, fmt.Sprintf("collector %q: max_query_text_length must be <= %d", name, MaxQueryTextLength))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("collectors config validation:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}
