package patroni

var PatroniManagedParameters = map[string]bool{
	"hot_standby":               true,
	"in_hot_standby":            true,
	"primary_conninfo":          true,
	"primary_slot_name":         true,
	"recovery_min_apply_delay":  true,
	"synchronous_standby_names": true,
	"transaction_read_only":     true,
	"max_connections":           true,
	"max_wal_senders":           true,
	"wal_level":                 true,
	"max_replication_slots":     true,
	"cluster_name":              true,
}

func IsPatroniManagedParameter(name string) bool {
	return PatroniManagedParameters[name]
}
