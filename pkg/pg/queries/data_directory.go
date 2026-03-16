package queries

// DataDirectory queries the filesystem path of the PostgreSQL data directory (PGDATA).
//
// Used to locate configuration files and WAL segments on disk, primarily by
// adapters that need direct filesystem access (e.g. pgprem for pg_ctl operations).
//
// https://www.postgresql.org/docs/current/runtime-config-file-locations.html#GUC-DATA-DIRECTORY

import (
	"context"

	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DataDirectoryQuery = `SHOW data_directory`

func DataDirectory(pgPool *pgxpool.Pool) (string, error) {
	var dataDir string
	err := utils.QueryRowWithPrefix(pgPool, context.Background(), DataDirectoryQuery).Scan(&dataDir)
	if err != nil {
		return "", err
	}
	return dataDir, nil
}
