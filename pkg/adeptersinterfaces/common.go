package adeptersinterfaces

import (
	"github.com/docker/docker/client"
	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// Common interface for all PostgreSQL adapters
// In the future if we support other databases, we can add
// a common interface for all database adapters and attach the driver and anything shared

// PostgreSQLAdapter is a struct that embeds the
// CommonAgent and adds a PGDriver. This is used by all the PostgreSQL adapters.
type PostgreSQLAdapter interface {
	Logger() *logrus.Logger
	PGDriver() *pgPool.Pool
	APIClient() *retryablehttp.Client
}

type DockerAdapter interface {
	PostgreSQLAdapter
	GetContainerName() string
	GetDockerClient() *client.Client
}
