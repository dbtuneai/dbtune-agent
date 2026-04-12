package queries

import "github.com/dbtuneai/agent/pkg/pg/collectorconfig"

// CatalogRegistrations returns all catalog collector registrations.
// Each collector file defines its own registration variable; this function
// assembles them into a single slice for the central config loader.
func CatalogRegistrations() []collectorconfig.CollectorRegistration {
	return []collectorconfig.CollectorRegistration{
		AutovacuumCountRegistration,
		ConnectionStatsRegistration,
		DatabaseSizeRegistration,
		PgAttributeRegistration,
		PgClassRegistration,
		PgDatabaseRegistration,
		PgIndexRegistration,
		PgLocksRegistration,
		PgPreparedXactsRegistration,
		PgReplicationSlotsRegistration,
		PgStatActivityRegistration,
		PgStatArchiverRegistration,
		PgStatBgwriterRegistration,
		PgStatCheckpointerRegistration,
		PgStatDatabaseRegistration,
		PgStatDatabaseConflictsRegistration,
		PgStatIORegistration,
		PgStatProgressAnalyzeRegistration,
		PgStatProgressCreateIndexRegistration,
		PgStatProgressVacuumRegistration,
		PgStatRecoveryPrefetchRegistration,
		PgStatReplicationRegistration,
		PgStatReplicationSlotsRegistration,
		PgStatSlruRegistration,
		PgStatStatementsRegistration,
		PgStatSubscriptionRegistration,
		PgStatSubscriptionStatsRegistration,
		PgStatUserFunctionsRegistration,
		PgStatUserIndexesRegistration,
		PgStatUserTablesRegistration,
		PgStatWalRegistration,
		PgStatWalReceiverRegistration,
		PgStatioUserIndexesRegistration,
		PgStatioUserTablesRegistration,
		PgStatsRegistration,
		TransactionCommitsRegistration,
		UptimeMinutesRegistration,
		WaitEventsRegistration,
	}
}
