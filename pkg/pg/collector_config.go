package pg

import (
	"fmt"
	"strings"
	"time"
)

// CollectorOverride holds per-collector configuration that can be set via
// the collectors section in dbtune.yaml or via environment variables.
type CollectorOverride struct {
	Enabled            *bool `mapstructure:"enabled"`
	IntervalSeconds    *int  `mapstructure:"interval_seconds"`
	IncludeQueries     *bool `mapstructure:"include_queries"`
	MaxQueryTextLength *int  `mapstructure:"max_query_text_length"`
	DiffLimit          *int  `mapstructure:"diff_limit"`
	IncludeTableData   *bool `mapstructure:"include_table_data"`
	BackfillBatchSize  *int  `mapstructure:"backfill_batch_size"`
	CategoryLimit      *int  `mapstructure:"category_limit"`
}

// CollectorsConfig maps collector names to their overrides.
type CollectorsConfig map[string]CollectorOverride

// IsEnabled returns true if the collector is not explicitly disabled.
func (o CollectorOverride) IsEnabled() bool {
	if o.Enabled == nil {
		return true
	}
	return *o.Enabled
}

// IntervalOr returns the configured interval clamped to at least def.
// Users can slow down collection but not speed it up beyond the default.
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

// IntOr returns *ptr if non-nil, otherwise def.
func IntOr(ptr *int, def int) int {
	if ptr == nil {
		return def
	}
	return *ptr
}

// BoolOr returns *ptr if non-nil, otherwise def.
func BoolOr(ptr *bool, def bool) bool {
	if ptr == nil {
		return def
	}
	return *ptr
}

const (
	MaxDiffLimit       = 500
	MaxQueryTextLength = 8192
)

// knownCollectorFields maps each known collector name to its valid extra fields
// (beyond the universal "enabled" and "interval_seconds").
var knownCollectorFields = map[string][]string{
	// Metric collectors
	"database_average_query_runtime":   {"include_queries", "max_query_text_length"},
	"database_transactions_per_second": nil,
	"database_connections":             nil,
	"system_db_size":                   nil,
	"database_autovacuum_count":        nil,
	"server_uptime":                    nil,
	"pg_database":                      nil,
	"pg_user_tables":                   nil,
	"pg_bgwriter":                      nil,
	"pg_wal":                           nil,
	"database_wait_events":             nil,
	"hardware":                         nil,
	"pg_checkpointer":                  nil,

	// Catalog collectors
	"autovacuum_count":              nil,
	"database_transactions":         nil,
	"pg_attribute":                  nil,
	"pg_class":                      {"backfill_batch_size"},
	"pg_index":                      nil,
	"pg_locks":                      nil,
	"pg_prepared_xacts":             nil,
	"pg_replication_slots":          nil,
	"pg_stat_activity":              nil,
	"pg_stat_archiver":              nil,
	"pg_stat_bgwriter":              nil,
	"pg_stat_checkpointer":          nil,
	"pg_stat_database":              nil,
	"pg_stat_database_conflicts":    nil,
	"pg_stat_io":                    nil,
	"pg_stat_progress_analyze":      nil,
	"pg_stat_progress_create_index": nil,
	"pg_stat_progress_vacuum":       nil,
	"pg_stat_replication":           nil,
	"pg_stat_replication_slots":     nil,
	"pg_stat_recovery_prefetch":     nil,
	"pg_stat_slru":                  nil,
	"pg_stat_statements":            {"diff_limit", "include_queries", "max_query_text_length"},
	"pg_stat_subscription":          nil,
	"pg_stat_subscription_stats":    nil,
	"pg_stat_user_functions":        nil,
	"pg_stat_user_indexes":          {"category_limit"},
	"pg_stat_user_tables":           {"category_limit"},
	"pg_stat_wal":                   nil,
	"pg_stat_wal_receiver":          nil,
	"pg_statio_user_indexes":        {"backfill_batch_size"},
	"pg_statio_user_tables":         nil,
	"pg_stats":                      {"backfill_batch_size", "include_table_data"},
	"wait_events":                   nil,
}

func validateCollectorsConfig(cfg CollectorsConfig) error {
	var errs []string

	for name, override := range cfg {
		extraFields, known := knownCollectorFields[name]
		if !known {
			errs = append(errs, fmt.Sprintf("unknown collector %q", name))
			continue
		}

		allowed := make(map[string]bool, len(extraFields))
		for _, f := range extraFields {
			allowed[f] = true
		}

		if override.IntervalSeconds != nil && *override.IntervalSeconds < 0 {
			errs = append(errs, fmt.Sprintf("collector %q: interval_seconds must be >= 0", name))
		}

		if override.DiffLimit != nil {
			if !allowed["diff_limit"] {
				errs = append(errs, fmt.Sprintf("collector %q: diff_limit is not a valid field", name))
			} else if *override.DiffLimit < 0 || *override.DiffLimit > MaxDiffLimit {
				errs = append(errs, fmt.Sprintf("collector %q: diff_limit must be between 0 and %d", name, MaxDiffLimit))
			}
		}

		if override.MaxQueryTextLength != nil {
			if !allowed["max_query_text_length"] {
				errs = append(errs, fmt.Sprintf("collector %q: max_query_text_length is not a valid field", name))
			} else if *override.MaxQueryTextLength < 0 || *override.MaxQueryTextLength > MaxQueryTextLength {
				errs = append(errs, fmt.Sprintf("collector %q: max_query_text_length must be between 0 and %d", name, MaxQueryTextLength))
			}
		}

		if override.BackfillBatchSize != nil {
			if !allowed["backfill_batch_size"] {
				errs = append(errs, fmt.Sprintf("collector %q: backfill_batch_size is not a valid field", name))
			} else if *override.BackfillBatchSize < 0 {
				errs = append(errs, fmt.Sprintf("collector %q: backfill_batch_size must be >= 0", name))
			}
		}

		if override.CategoryLimit != nil {
			if !allowed["category_limit"] {
				errs = append(errs, fmt.Sprintf("collector %q: category_limit is not a valid field", name))
			} else if *override.CategoryLimit < 0 {
				errs = append(errs, fmt.Sprintf("collector %q: category_limit must be >= 0", name))
			}
		}

		if override.IncludeQueries != nil && !allowed["include_queries"] {
			errs = append(errs, fmt.Sprintf("collector %q: include_queries is not a valid field", name))
		}

		if override.IncludeTableData != nil && !allowed["include_table_data"] {
			errs = append(errs, fmt.Sprintf("collector %q: include_table_data is not a valid field", name))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("collectors config validation failed:\n  %s", strings.Join(errs, "\n  "))
	}

	return nil
}
