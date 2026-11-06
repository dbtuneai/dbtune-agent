package tuning

import (
	"context"
	"strconv"

	"github.com/dbtuneai/agent/pkg/catalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// inferNumericType attempts to parse a string setting into a numeric Go type.
// Returns int64 if parseable as integer, float64 if parseable as float, or the
// original string if neither. This preserves JSON serialization format (200 not "200")
// without relying on the potentially unreliable vartype metadata.
func InferNumericType(setting interface{}) interface{} {
	s, ok := setting.(string)
	if !ok {
		return setting
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func GetActiveConfig(
	pool *pgxpool.Pool,
	ctx context.Context,
) (ConfigArraySchema, error) {
	settings, err := catalog.QueryPgSettings(pool, ctx)
	if err != nil {
		return nil, err
	}
	return settingsToConfigRows(settings), nil
}

// settingsToConfigRows converts raw PgSettingsRow values into the
// ConfigArraySchema consumed by the tuning loop. Numeric vartypes
// have InferNumericType applied; NULL units become nil.
func settingsToConfigRows(settings []catalog.PgSettingsRow) ConfigArraySchema {
	configRows := make(ConfigArraySchema, 0, len(settings))
	for _, s := range settings {
		var unit interface{}
		if s.Unit != nil {
			unit = string(*s.Unit)
		}

		var setting interface{} = string(s.Setting)
		if s.Vartype == "real" || s.Vartype == "integer" {
			setting = InferNumericType(setting)
		}

		configRows = append(configRows, PGConfigRow{
			Name:    string(s.Name),
			Setting: setting,
			Unit:    unit,
			Vartype: string(s.Vartype),
			Context: string(s.Context),
		})
	}
	return configRows
}
