package adeptersinterfaces

import (
	"time"

	aiven "github.com/aiven/go-client-codegen"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	"github.com/aws/aws-sdk-go-v2/service/rds"
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

type AuroraPerformanceInsightsState struct {
	Enabled     bool
	ResourceID  string
	LastChecked time.Time
}

type AuroraHardwareState struct {
	TotalMemoryBytes int64
	NumCPUs          int
	LastChecked      time.Time
}

type AuroraRDSState struct {
	LastAppliedConfig   time.Time
	PerformanceInsights *AuroraPerformanceInsightsState
	Hardware            *AuroraHardwareState
	LastGuardrailCheck  time.Time
}

type AuroraRDSConfig struct {
	AWSAccessKey          string `mapstructure:"AWS_ACCESS_KEY_ID"`
	AWSSecretAccessKey    string `mapstructure:"AWS_SECRET_ACCESS_KEY"`
	AWSRegion             string `mapstructure:"AWS_REGION" validate:"required"`
	RDSDatabaseIdentifier string `mapstructure:"RDS_DATABASE_IDENTIFIER" validate:"required"`
	RDSParameterGroupName string `mapstructure:"RDS_PARAMETER_GROUP_NAME" validate:"required"`
}

type AuroraRDSAdapter interface {
	PostgreSQLAdapter
	GetPIClient() *pi.Client
	GetEC2Client() *ec2.Client
	GetRDSClient() *rds.Client
	GetCWClient() *cloudwatch.Client
	GetAuroraRDSConfig() *AuroraRDSConfig
	GetEC2InstanceTypeInfo() (*ec2types.InstanceTypeInfo, error)
	GetAuroraState() *AuroraRDSState
}

// Aiven interfaces
type AivenHardwareState struct {
	TotalMemoryBytes int64
	NumCPUs          int
	LastChecked      time.Time
}

type AivenState struct {
	Hardware                       *AivenHardwareState
	InitialSharedBuffersPercentage float64
	InitialWorkMem                 int64
	LastAppliedConfig              time.Time
	// HACK: Used to trigger restarts on ALTER DATABASE statements
	LastKnownPGStatMonitorEnable bool
	// Guardrails
	LastGuardrailCheck            time.Time
	LastMemoryAvailableTime       time.Time
	LastMemoryAvailablePercentage float64
	LastHardwareInfoTime          time.Time
}

type AivenConfig struct {
	APIToken                string        `mapstructure:"AIVEN_API_TOKEN" validate:"required"`
	ProjectName             string        `mapstructure:"AIVEN_PROJECT_NAME" validate:"required"`
	ServiceName             string        `mapstructure:"AIVEN_SERVICE_NAME" validate:"required"`
	MetricResolutionSeconds time.Duration `mapstructure:"metric_resolution_seconds" validate:"required"`
	// NOTE: If specified, we are able to use the
	// session refresh hack. Not documented.
	DatabaseName string `mapstructure:"database_name"`
}

type AivenPostgreSQLAdapter interface {
	PostgreSQLAdapter
	GetAivenClient() *aiven.Client
	GetAivenConfig() *AivenConfig
	GetAivenState() *AivenState
}
