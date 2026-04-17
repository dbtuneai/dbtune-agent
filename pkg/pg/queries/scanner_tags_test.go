package queries

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// scannerBackedTypes lists all struct types scanned by pgxutil.Scanner
// (via NewScanner[T] or NewCollector[T]). Every exported field must have
// a `db:"column_name"` tag so the scanner maps PostgreSQL columns correctly.
// Without an explicit tag the scanner falls back to the lowercased Go field
// name, which silently fails to populate the field when the two don't match
// (e.g., DataName → "dataname" vs column "datname").
//
// Excluded: PgStatsRow (uses manual rows.Scan, not pgxutil.Scanner).
var scannerBackedTypes = []interface{}{
	AutovacuumCountRow{},
	ConnectionStatsRow{},
	DatabaseSizeRow{},
	PgAttributeRow{},
	PgClassRow{},
	PgDatabaseRow{},
	PgIndexRow{},
	PgLocksRow{},
	PgPreparedXactsRow{},
	PgReplicationSlotsRow{},
	PgSettingsRow{},
	PgStatActivityRow{},
	PgStatArchiverRow{},
	PgStatBgwriterRow{},
	PgStatCheckpointerRow{},
	PgStatDatabaseConflictsRow{},
	PgStatDatabaseRow{},
	PgStatIORow{},
	PgStatProgressAnalyzeRow{},
	PgStatProgressCreateIndexRow{},
	PgStatProgressVacuumRow{},
	PgStatRecoveryPrefetchRow{},
	PgStatReplicationRow{},
	PgStatReplicationSlotsRow{},
	PgStatSlruRow{},
	PgStatStatementsRow{},
	PgStatSubscriptionRow{},
	PgStatSubscriptionStatsRow{},
	PgStatUserFunctionsRow{},
	PgStatUserTableRow{},
	PgStatUserIndexesRow{},
	PgStatWalRow{},
	PgStatWalReceiverRow{},
	PgStatioUserIndexesRow{},
	PgStatioUserTablesRow{},
	UptimeMinutesRow{},
	WaitEventRow{},
}

// TestScannerStructs_ColumnMapping ensures every exported field on
// scanner-backed structs maps correctly to a PostgreSQL column. A field
// is correct if either:
//   - it has an explicit `db:"column"` tag, or
//   - its lowercased Go name already matches the `json` tag (the column name).
func TestScannerStructs_ColumnMapping(t *testing.T) {
	for _, v := range scannerBackedTypes {
		typ := reflect.TypeOf(v)
		t.Run(typ.Name(), func(t *testing.T) {
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				if !f.IsExported() {
					continue
				}
				dbTag := f.Tag.Get("db")
				if dbTag != "" {
					continue // explicit db tag — scanner will use it
				}
				jsonTag := f.Tag.Get("json")
				if jsonTag == "" {
					continue
				}
				// Strip json options (e.g. ",omitempty")
				if idx := strings.Index(jsonTag, ","); idx != -1 {
					jsonTag = jsonTag[:idx]
				}
				assert.Equal(t, jsonTag, strings.ToLower(f.Name),
					"field %s.%s: lowercased name %q doesn't match json tag %q — add a db tag or rename the field",
					typ.Name(), f.Name, strings.ToLower(f.Name), jsonTag)
			}
		})
	}
}
