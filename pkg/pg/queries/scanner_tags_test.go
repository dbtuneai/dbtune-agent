package queries

import (
	"reflect"
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

// TestScannerStructs_RequireDBTags ensures every exported field on
// scanner-backed structs has an explicit db tag. This prevents silent
// scan failures where the lowercased Go name doesn't match the PG column.
func TestScannerStructs_RequireDBTags(t *testing.T) {
	for _, v := range scannerBackedTypes {
		typ := reflect.TypeOf(v)
		t.Run(typ.Name(), func(t *testing.T) {
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				if !f.IsExported() {
					continue
				}
				tag := f.Tag.Get("db")
				assert.NotEmpty(t, tag,
					"field %s.%s must have a db tag for pgxutil.Scanner column mapping",
					typ.Name(), f.Name)
			}
		})
	}
}
