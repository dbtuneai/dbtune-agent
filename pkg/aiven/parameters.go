package aiven

type ModifyLevel string

const (
	ModifyServiceLevel ModifyLevel = "service_level"  // Can modify via service level config Aiven API
	ModifyUserPGConfig ModifyLevel = "user_pg_config" // Can modify via user config Aiven API, prefer over ModifyAlterDB, no restart
	ModifyAlterDB      ModifyLevel = "alter_db"       // Can modify with ALTER DATABASE <dbname> SET <param> = <value>, requires restart
	NoModify           ModifyLevel = "no_modify"      // Can not modify at all
)

// Ideally, we can remove the restart from most of these
var aivenModifiableParams = map[string]struct {
	ModifyLevel     ModifyLevel
	RequiresRestart bool
}{
	"shared_buffers_percentage":       {ModifyServiceLevel, true},
	"work_mem":                        {ModifyServiceLevel, false},
	"bgwriter_lru_maxpages":           {ModifyUserPGConfig, false},
	"bgwriter_delay":                  {ModifyUserPGConfig, false},
	"max_parallel_workers_per_gather": {ModifyUserPGConfig, false},
	"max_parallel_workers":            {ModifyUserPGConfig, false},
	"random_page_cost":                {ModifyAlterDB, true},
	"seq_page_cost":                   {ModifyAlterDB, true},
	"effective_io_concurrency":        {ModifyAlterDB, true},
	// TODO: Get these to be modifiable?
	"checkpoint_completion_target": {NoModify, false},
	"max_wal_size":                 {NoModify, false},
	"min_wal_size":                 {NoModify, false},
	"shared_buffers":               {NoModify, false}, // Done through shared_buffers_percentage
	"max_worker_processes":         {NoModify, false}, // BUG: Cannot decrease on Aiven's end
}
