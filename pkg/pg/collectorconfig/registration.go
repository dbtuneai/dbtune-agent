package collectorconfig

// CollectorRegistration describes a known collector and how to parse its config.
type CollectorRegistration struct {
	Name string
	Kind CollectorKind

	// AllowedFields lists the extra field keys this collector accepts
	// (beyond the universal "enabled" and "interval_seconds").
	AllowedFields []string

	// ParseConfig parses collector-specific config from a raw field map.
	// Returns a typed config value (e.g. PgClassConfig) or nil for
	// collectors with no extra options.
	// The raw map contains only the extra fields (universal fields are
	// handled separately by the central parser).
	ParseConfig func(raw map[string]any) (any, error)
}
