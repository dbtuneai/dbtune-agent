package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/aiven"
	"github.com/dbtuneai/agent/pkg/docker"
	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/dbtuneai/agent/pkg/pgprem"
	"github.com/dbtuneai/agent/pkg/rds"
	"github.com/dbtuneai/agent/pkg/runner"
	"github.com/spf13/viper"
)

const AVAILABLE_FLAGS = "--docker, --aurora, --rds, --aiven, --local"

func main() {
	// Define flags
	useDocker := flag.Bool("docker", false, "Use Docker adapter")
	useAurora := flag.Bool("aurora", false, "Use Aurora adapter")
	useRDS := flag.Bool("rds", false, "Use RDS adapater")
	useAiven := flag.Bool("aiven", false, "Use Aiven PostgreSQL adapter")
	useLocal := flag.Bool("local", false, "Use local PostgreSQL adapter")
	flag.Parse()

	// Set the file name of the configurations file
	viper.SetConfigName("dbtune")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".") // optionally look for config in the working directory

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
	case *useLocal:
		adapter, err = pgprem.CreateDefaultPostgreSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create local PostgreSQL adapter: %v", err)
		}
	default:
		provider, err := detectProvider()
		if err != nil {
			log.Fatal(
				"No provider detected, please check your environemnt variables or specify one of the following flags: " + AVAILABLE_FLAGS,
			)
		}
		log.Printf("Detected config for %s adapter", provider)
		switch provider {
		case "docker":
			adapter, err = docker.CreateDockerContainerAdapter()
		case "rds":
			adapter, err = rds.CreateRDSAdapter(nil)
		case "aurora":
			adapter, err = rds.CreateAuroraRDSAdapter()
		case "aiven":
			adapter, err = aiven.CreateAivenPostgreSQLAdapter()
		case "local":
			adapter, err = pgprem.CreateDefaultPostgreSQLAdapter()
		default:
			log.Fatal("No provider detected, please check your environemnt variables or specify one of the following flags: " + AVAILABLE_FLAGS)
		}
		if err != nil {
			log.Fatalf(
				"Failed to create %s adapter. If you meant for a different adapter, please check your config and/or environment variables or specify one of the following flags: "+AVAILABLE_FLAGS+". Error: %v",
				provider,
				err,
			)
		}
	}
	runner.Runner(adapter)
}

// If no explicit provider is specified, we go through the list of available adapters
// and see if we can initiate a successful configuration for any of them. With how we
// have things structured, this is almost always going to be one of them as 'local'
// matches off just the `postgres:` in the config file.
func detectProvider() (string, error) {
	var err error

	_, err = docker.ConfigFromViper(nil)
	if err == nil {
		return "docker", nil
	}

	_, err = rds.ConfigFromViper(rds.RDS_CONFIG_KEY)
	if err == nil {
		return "rds", nil
	}

	_, err = rds.ConfigFromViper(rds.AURORA_CONFIG_KEY)
	if err == nil {
		return "aurora", nil
	}

	_, err = aiven.ConfigFromViper(nil)
	if err == nil {
		return "aiven", nil
	}
	log.Printf("aiven adapter error: %v", err)

	// NOTE: Important this goes last, as it normally always exists as we usually
	// have the `postgres: in the config file`
	_, err = pg.ConfigFromViper(nil)
	if err == nil {
		return "local", nil
	}

	return "", fmt.Errorf("no provider detected")
}
