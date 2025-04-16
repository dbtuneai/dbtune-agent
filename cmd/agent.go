package main

import (
	"flag"
	"log"

	"github.com/dbtuneai/agent/pkg/adapters"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/runner"
	"github.com/spf13/viper"
)

func main() {
	// Define flags
	useDocker := flag.Bool("docker", false, "Use Docker adapter")
	useAurora := flag.Bool("aurora", false, "Use Aurora adapter")
	useAiven := flag.Bool("aiven", false, "Use Aiven PostgreSQL adapter")
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
		adapter, err = adapters.CreateDockerContainerAdapter()
		if err != nil {
			log.Fatalf("Failed to create Docker adapter: %v", err)
		}
	case *useAurora:
		adapter, err = adapters.CreateAuroraRDSAdapter()
		if err != nil {
			log.Fatalf("Failed to create Aurora RDS adapter: %v", err)
		}
	case *useAiven:
		adapter, err = adapters.CreateAivenPostgreSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create Aiven PostgreSQL adapter: %v", err)
		}
	default:
		adapter, err = adapters.CreateDefaultPostgreSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create PostgreSQL adapter: %v", err)
		}
	}

	runner.Runner(adapter)
}
