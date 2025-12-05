package cnpg

import (
	"fmt"
	"strconv"

	"github.com/dbtuneai/agent/pkg/agent"
)

// CNPGManagedParameters are parameters that CNPG's webhook validation explicitly
// prevents from being modified, or that CNPG manages internally and should not be
// overridden by tuning configurations.
//
// We MUST filter them out to prevent breaking the cluster.
//
// Example: shared_preload_libraries="" would remove pg_stat_statements and break metrics!
//
// Source: https://cloudnative-pg.io/documentation/current/postgresql_conf/
var CNPGManagedParameters = map[string]bool{
	// Archive and Recovery - CNPG manages WAL archiving
	"archive_mode":              true,
	"archive_command":           true,
	"archive_cleanup_command":   true,
	"restore_command":           true,
	"recovery_end_command":      true,
	"recovery_target":           true,
	"recovery_target_action":    true,
	"recovery_target_inclusive": true,
	"recovery_target_lsn":       true,
	"recovery_target_name":      true,
	"recovery_target_time":      true,
	"recovery_target_timeline":  true,
	"recovery_target_xid":       true,

	// Replication and High Availability
	"hot_standby":               true,
	"primary_conninfo":          true,
	"primary_slot_name":         true,
	"recovery_min_apply_delay":  true,
	"synchronous_standby_names": true,

	// Network and Connection Settings
	"listen_addresses":        true,
	"port":                    true,
	"unix_socket_directories": true,
	"unix_socket_group":       true,
	"unix_socket_permissions": true,

	// SSL/TLS Configuration - CNPG manages certificates
	"ssl":                                    true,
	"ssl_ca_file":                            true,
	"ssl_cert_file":                          true,
	"ssl_crl_file":                           true,
	"ssl_dh_params_file":                     true,
	"ssl_ecdh_curve":                         true,
	"ssl_key_file":                           true,
	"ssl_passphrase_command":                 true,
	"ssl_passphrase_command_supports_reload": true,
	"ssl_prefer_server_ciphers":              true,

	// Logging - CNPG uses centralized JSON logging
	"log_destination":          true, // Backend sends this
	"log_directory":            true, // Backend sends this
	"log_file_mode":            true,
	"log_filename":             true, // Backend sends this
	"log_rotation_age":         true, // Backend sends this
	"log_rotation_size":        true, // Backend sends this
	"log_truncate_on_rotation": true, // Backend sends this
	"logging_collector":        true, // Backend sends this
	"stats_temp_directory":     true,

	// Syslog settings
	"syslog_facility":         true,
	"syslog_ident":            true,
	"syslog_sequence_numbers": true,
	"syslog_split_messages":   true,

	// System and File Paths
	"cluster_name":         true,
	"config_file":          true,
	"data_directory":       true,
	"hba_file":             true,
	"ident_file":           true,
	"external_pid_file":    true,
	"promote_trigger_file": true,

	// System Behavior
	"restart_after_crash":     true,
	"allow_alter_system":      true,
	"allow_system_table_mods": true,
	"data_sync_retry":         true,
	"jit_provider":            true,

	// CRITICAL: Extensions - CNPG auto-manages for pg_stat_statements, pgaudit, etc.
	// Backend sent "" which would REMOVE pg_stat_statements!
	"shared_preload_libraries": true,

	// Windows-specific
	"event_source": true,

	// Service discovery (macOS-specific)
	"bonjour":      true,
	"bonjour_name": true,
}

// IsCNPGManagedParameter returns true if the parameter is managed by CNPG
// and should not be modified by the tuning agent.
func IsCNPGManagedParameter(name string) bool {
	return CNPGManagedParameters[name]
}

// PostgreSQL memory parameters and their native units
// These parameters need to be converted to human-readable format for CNPG
var memoryParameterUnits = map[string]string{
	// 8kB block parameters
	"shared_buffers":       "8kB",
	"effective_cache_size": "8kB",
	"wal_buffers":          "8kB",
	"temp_buffers":         "8kB",

	// kB parameters
	"work_mem":             "kB",
	"maintenance_work_mem": "kB",
	"autovacuum_work_mem":  "kB",

	// MB parameters
	"max_wal_size": "MB",
	"min_wal_size": "MB",
}

// ConvertToCNPGFormat converts PostgreSQL parameter values to CNPG human-readable format.
// CNPG expects memory values like "2GB", "256MB" instead of raw block counts like "262144".
//
// For non-memory parameters, returns the original value unchanged.
func ConvertToCNPGFormat(name string, value string, knobConfig *agent.PGConfigRow) string {
	unit, isMemoryParam := memoryParameterUnits[name]
	if !isMemoryParam {
		return value
	}

	return ConvertWithUnit(value, unit)
}

// ConvertWithUnit converts a value with a given unit to CNPG format.
// Example: value="262144", unit="8kB" → "2GB"
func ConvertWithUnit(value, unit string) string {
	// Parse the numeric value
	numValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		// If we can't parse, return original value
		return value
	}

	// Convert to bytes based on the unit
	var bytes int64
	switch unit {
	case "8kB":
		bytes = numValue * 8 * 1024
	case "kB":
		bytes = numValue * 1024
	case "MB":
		bytes = numValue * 1024 * 1024
	case "GB":
		bytes = numValue * 1024 * 1024 * 1024
	default:
		return value
	}

	// Convert bytes to human-readable format
	return bytesToHumanReadable(bytes)
}

// bytesToHumanReadable converts bytes to kB format for CNPG.
// Always returns values in kB (e.g., "2097152kB" for 2GB)
func bytesToHumanReadable(bytes int64) string {
	const KB = 1024
	return fmt.Sprintf("%dkB", bytes/KB)
}
