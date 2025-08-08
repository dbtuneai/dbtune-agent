package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/aiven"
	"github.com/dbtuneai/agent/pkg/checks"
	"github.com/dbtuneai/agent/pkg/cloudsql"
	"github.com/dbtuneai/agent/pkg/docker"
	"github.com/dbtuneai/agent/pkg/pgprem"
	"github.com/dbtuneai/agent/pkg/rds"
	"github.com/dbtuneai/agent/pkg/runner"
	"github.com/dbtuneai/agent/pkg/version"
	"github.com/spf13/viper"
)

const AVAILABLE_FLAGS = "--docker, --aurora, --rds, --aiven, --local, --cloudsql"

func main() {
	// Define flags
	useDocker := flag.Bool("docker", false, "Use Docker adapter")
	useAurora := flag.Bool("aurora", false, "Use Aurora adapter")
	useRDS := flag.Bool("rds", false, "Use RDS adapter")
	useAiven := flag.Bool("aiven", false, "Use Aiven PostgreSQL adapter")
	useLocal := flag.Bool("local", false, "Use local PostgreSQL adapter")
	useCloudSQL := flag.Bool("cloudsql", false, "Use Cloud SQL adapter")
	showVersion := flag.Bool("version", false, "Show version information")
	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Println(version.GetVersion())
		os.Exit(0)
	}

	// Set the file name of the configurations file
	viper.SetConfigName("dbtune")
	viper.SetConfigType("yaml")

	// Locations where to look for the config file
	viper.AddConfigPath("/etc/")        // config at /etc/dbtune.yaml
	viper.AddConfigPath("/etc/dbtune/") // config at /etc/dbtune/dbtune.yaml
	viper.AddConfigPath(".")            // optionally look for config in the working directory

	// Read the configuration file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore the error
			log.Println("No config file found, proceeding with environment variables only.")
		} else {
			// Config file was found but another error occurred
			log.Fatalf("Error reading config file, %s", err)
		}
	}

	viper.AutomaticEnv()      // Read also environment variables
	viper.SetEnvPrefix("DBT") // Set a prefix for environment variables

	// Startup checks for config and connectivity
	disableChecks := viper.GetBool("disable_checks")
	if !disableChecks {
		if err := checks.CheckStartupRequirements(); err != nil {
			log.Fatalf("Startup check failed: %v", err)
		}
	} else {
		log.Println("Startup checks are disabled via configuration (disable_checks=true)")
	}

	var adapter agent.AgentLooper
	var err error

	// Create the appropriate adapter based on flags
	switch {
	case *useDocker:
		adapter, err = docker.CreateDockerContainerAdapter()
		if err != nil {
			log.Fatalf("Failed to create Docker adapter: %v", err)
		}
	case *useRDS:
		adapter, err = rds.CreateRDSAdapter(nil)
		if err != nil {
			log.Fatalf("Failed to create Aurora RDS adapter: %v", err)
		}
	case *useAurora:
		adapter, err = rds.CreateAuroraRDSAdapter()
		if err != nil {
			log.Fatalf("Failed to create Aurora RDS adapter: %v", err)
		}
	case *useAiven:
		adapter, err = aiven.CreateAivenPostgreSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create Aiven PostgreSQL adapter: %v", err)
		}
	case *useCloudSQL:
		adapter, err = cloudsql.CreateCloudSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create Cloud SQL PostgreSQL adapter: %v", err)
		}
	case *useLocal:
		adapter, err = pgprem.CreateDefaultPostgreSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create local PostgreSQL adapter: %v", err)
		}
	default:
		log.Println("No explicit provider specified, detecting config present...")
		log.Println("To explicitly specify a provider, please use one of the following flags: " + AVAILABLE_FLAGS)

		// We first check for environment variables as they should take precedence over a config file.
		if aiven.DetectConfigFromEnv() {
			log.Println("Aiven PostgreSQL adapter detected from environment variables")
			adapter, err = aiven.CreateAivenPostgreSQLAdapter()

		} else if docker.DetectConfigFromEnv() {
			log.Println("Docker container adapter detected from environment variables")
			adapter, err = docker.CreateDockerContainerAdapter()

		} else if rds.DetectConfigFromEnv() {
			// NOTE: This is because they both share the same environment variables and there's
			// no easy distinction. Theoretically we could just let this run, as they have no functional
			// differences, but better not to introduce this default behaviour incase it was to change.
			log.Println("RDS or Aurora configuration detected")
			log.Fatal(
				"Ambiguous configuration detected. This can happen if using environment variables, as they" +
					" are used for both RDS and Aurora. Please specify which using `--rds` or `--aurora`.",
			)

		} else if aiven.DetectConfigFromConfigFile() {
			log.Println("Aiven PostgreSQL configuration detected in config file")
			adapter, err = aiven.CreateAivenPostgreSQLAdapter()

		} else if docker.DetectConfigFromConfigFile() {
			log.Println("Docker container configuration detected in config file")
			adapter, err = docker.CreateDockerContainerAdapter()

		} else if configType := rds.DetectConfigFromConfigFile(); configType == rds.RDS || configType == rds.Aurora {
			switch configType {
			case rds.RDS:
				log.Println("RDS configuration detected in config file")
				adapter, err = rds.CreateRDSAdapter(nil)
			case rds.Aurora:
				log.Println("Aurora configuration detected in config file")
				adapter, err = rds.CreateAuroraRDSAdapter()
			}
		} else {
			// NOTE: This was the previous behavior, which is consistent with our configuration.
			// All config files have the `postgres:` subheader, which is all the local Postgres
			// adapter requires. I would rather error out but alas.
			log.Println("Defaulting to local PostgreSQL adapter as no other adapter was detected.")
			adapter, err = pgprem.CreateDefaultPostgreSQLAdapter()
		}
		if err != nil {
			log.Fatalf("Failed to create adapter: %v", err)
		}
	}
	runner.Runner(adapter)
}
