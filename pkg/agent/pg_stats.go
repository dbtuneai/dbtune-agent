package agent

import "encoding/json"

// PgStatsRow represents a single row from the pg_stats view,
// matching the backend's PgStats Django model.
type PgStatsRow struct {
	SchemaName          string          `json:"schemaname"`
	TableName           string          `json:"tablename"`
	AttName             string          `json:"attname"`
	Inherited           bool            `json:"inherited"`
	NullFrac            *float64        `json:"null_frac"`
	AvgWidth            *int            `json:"avg_width"`
	NDistinct           *float64        `json:"n_distinct"`
	MostCommonVals      json.RawMessage `json:"most_common_vals"`
	MostCommonFreqs     json.RawMessage `json:"most_common_freqs"`
	HistogramBounds     json.RawMessage `json:"histogram_bounds"`
	Correlation         *float64        `json:"correlation"`
	MostCommonElems     json.RawMessage `json:"most_common_elems"`
	MostCommonElemFreqs json.RawMessage `json:"most_common_elem_freqs"`
	ElemCountHistogram  json.RawMessage `json:"elem_count_histogram"`
}

// PgStatsPayload is the JSON body POSTed to /api/v1/agent/pg_stats.
type PgStatsPayload struct {
	Rows []PgStatsRow `json:"rows"`
}
