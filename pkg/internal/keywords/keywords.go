package keywords

// These align with what we show on the front-end or are otherwise needed.
// Please prefer to use these where possible
const (
	// CPU
	NodeCPUCount = "node_cpu_count"
	NodeCPUUsage = "node_cpu_usage"

	// Memory
	NodeMemoryTotal               = "node_memory_total"
	NodeMemoryUsed                = "node_memory_used"
	NodeMemoryUsedPercentage      = "node_memory_used_percentage"
	NodeMemoryFreeable            = "node_memory_freeable"
	NodeMemoryAvailablePercentage = "node_memory_available_percentage"

	// Load
	NodeLoadAverage = "node_load_average"
	// Load - NOTE: These are per second
	NodeDiskIOPSReadPerSecond  = "node_disk_iops_read"
	NodeDiskIOPSWritePerSecond = "node_disk_iops_write"
	NodeDiskIOPSTotalPerSecond = "node_disk_iops_total"
	// Load - NOTE: These are total count, as used in adapter pgprem
	NodeDiskIOReadCount  = "node_disk_io_ops_read"
	NodeDiskIOWriteCount = "node_disk_io_ops_write"
	NodeDiskIOTotalCount = "node_disk_io_ops_total"

	// Disk
	NodeDiskSize           = "node_disk_size"
	NodeDiskUsedPercentage = "node_disk_used_percentage"

	// Network
	NodeNetworkReceiveCount     = "node_net_receive_count"
	NodeNetworkSendCount        = "node_net_send_count"
	NodeNetworkReceivePerSecond = "node_net_receive_per_second"
	NodeNetworkSendPerSecond    = "node_net_send_per_second"

	// OS Info
	NodeOSInfo        = "node_os_info"
	NodeStorageType   = "node_storage_type"
	NodeOSPlatform    = "system_info_platform"
	NodeOSPlatformVer = "system_info_platform_version"

	// PG
	PGVersion                  = "pg_version"
	PGMaxConnections           = "pg_max_connections"
	PGStatStatementsDelta      = "pg_stat_statements_delta"
	PGStatStatementsDeltaCount = "pg_stat_statements_delta_count"
	PGActiveConnections        = "pg_active_connections"
	PGInstanceSize             = "pg_instance_size"
	PGAutovacuumCount          = "pg_autovacuum_count"
	PGCacheHitRatio            = "pg_cache_hit_ratio"
	PGWaitEventPrefix          = "pg_wait_event_"

	// Performance
	PerfAverageQueryRuntime   = "perf_average_query_runtime"
	PerfTransactionsPerSecond = "perf_transactions_per_second"

	// Misc
	ServerUptime = "server_uptime"
)
