package queries

// CollectPgSettings retrieves the full PostgreSQL runtime configuration from pg_settings.
//
// This function is NOT registered as a CatalogCollector because it is called directly
// from the config management goroutine (tied to the config apply loop), not on a
// periodic collection interval.
//
// InferNumericType is applied to every setting so that numeric values are returned
// as native Go numbers rather than strings, preserving correct JSON serialization
// (e.g. 200 instead of "200") without relying on the vartype metadata which can
// be unreliable.
//
// https://www.postgresql.org/docs/current/view-pg-settings.html

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// PGConfigRow represents a single row from pg_settings.
type PGConfigRow struct {
	Name    string `json:"name"`
	Setting any    `json:"setting"`
	Unit    any    `json:"unit"`
	Vartype string `json:"vartype"`
	Context string `json:"context"`
}

// GetSettingValue returns the setting value in its appropriate type and format.
// This is needed for cases like Aurora RDS when modifying parameters.
func (p PGConfigRow) GetSettingValue() (string, error) {
	switch p.Vartype {
	case "integer":
		var val int64
		switch v := p.Setting.(type) {
		case int:
			val = int64(v)
		case int64:
			val = v
		case float64:
			val = int64(v)
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return "", fmt.Errorf("failed to parse integer setting: %v", err)
			}
			val = parsed
		default:
			return "", fmt.Errorf("unexpected type for integer setting: %T", p.Setting)
		}
		return fmt.Sprintf("%d", val), nil

	case "real":
		var val float64
		switch v := p.Setting.(type) {
		case float64:
			val = v
		case int:
			val = float64(v)
		case int64:
			val = float64(v)
		case string:
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return "", fmt.Errorf("failed to parse real setting: %v", err)
			}
			val = parsed
		default:
			return "", fmt.Errorf("unexpected type for real setting: %T", p.Setting)
		}
		return fmt.Sprintf("%.6g", val), nil

	case "bool":
		return fmt.Sprintf("%v", p.Setting), nil

	case "string", "enum":
		return fmt.Sprintf("%v", p.Setting), nil

	default:
		return fmt.Sprintf("%v", p.Setting), nil
	}
}

const SelectAllSettings = `
SELECT name, setting, unit, vartype, context
FROM pg_settings
`

// InferNumericType attempts to parse a string setting into a numeric Go type.
// Returns int64 if parseable as integer, float64 if parseable as float, or the
// original string if neither. This preserves JSON serialization format (200 not "200")
// without relying on the potentially unreliable vartype metadata.
func InferNumericType(setting any) any {
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

func CollectPgSettings(
	pool *pgxpool.Pool,
	ctx context.Context,
	logger *logrus.Logger,
) ([]PGConfigRow, error) {
	rows, err := utils.QueryWithPrefix(pool, ctx, SelectAllSettings)
	if err != nil {
		return nil, err
	}

	var configRows []PGConfigRow
	for rows.Next() {
		var row PGConfigRow
		err := rows.Scan(&row.Name, &row.Setting, &row.Unit, &row.Vartype, &row.Context)
		if err != nil {
			logger.Error("Error scanning pg_settings row", err)
			continue
		}
		row.Setting = InferNumericType(row.Setting)
		configRows = append(configRows, row)
	}

	return configRows, nil
}
