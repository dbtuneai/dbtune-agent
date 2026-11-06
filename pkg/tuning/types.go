// Package tuning is the config loop: reading the active configuration,
// the recommendation the backend sends back, and applying it to PostgreSQL
// (ALTER SYSTEM, reload, restart policy).
package tuning

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type ConfigArraySchema []any

// TODO: extract PostgreSQL specific types + methods to utils/separate place
type PGConfigRow struct {
	Name    string      `json:"name"`
	Setting interface{} `json:"setting"`
	Unit    interface{} `json:"unit"`
	Vartype string      `json:"vartype"`
	Context string      `json:"context"`
}

// GetSettingValue returns the setting value in its appropriate type and format
// This is needed for cases like Aurora RDS when modifying parameters
func (p PGConfigRow) GetSettingValue() (string, error) {
	switch p.Vartype {
	case "integer":
		// Handle both string and number JSON representations
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
				return "", fmt.Errorf("failed to parse integer setting: %w", err)
			}
			val = parsed
		default:
			return "", fmt.Errorf("unexpected type for integer setting: %T", p.Setting)
		}
		return fmt.Sprintf("%d", val), nil

	case "real":
		// Handle both string and number JSON representations
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
				return "", fmt.Errorf("failed to parse real setting: %w", err)
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

// KnobApplication is the apply mode for a proposed configuration. Only the
// values declared as constants below are valid; any other value fails JSON
// decoding so adapters never see an unknown mode.
type KnobApplication string

const (
	KnobApplicationRestart KnobApplication = "restart"
	KnobApplicationReload  KnobApplication = "reload"
)

func (k *KnobApplication) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch KnobApplication(s) {
	case KnobApplicationRestart, KnobApplicationReload:
		*k = KnobApplication(s)
		return nil
	default:
		return fmt.Errorf("invalid knob_application %q (expected %q or %q)", s, KnobApplicationRestart, KnobApplicationReload)
	}
}

type ProposedConfigResponse struct {
	Config          []PGConfigRow   `json:"config"`
	KnobsOverrides  []string        `json:"knobs_overrides"`
	KnobApplication KnobApplication `json:"knob_application"`
}
