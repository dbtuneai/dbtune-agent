package main

import (
	"fmt"
	"log"
	"time"

	"example.com/dbtune-agent/internal/adapters"
	"example.com/dbtune-agent/internal/utils"
	"github.com/spf13/viper"
)

func runner(adapter utils.AgentLooper) {
	for {
		data, err := adapter.GetMetrics()
		if err != nil {
			panic(err)
		}

		err = adapter.SendMetrics(data)
		if err != nil {
			panic(err)
		}

		data, err = adapter.GetSystemInfo()
		if err != nil {
			panic(err)
		}

		err = adapter.SendSystemInfo(data)
		if err != nil {
			panic(err)
		}

		err = adapter.SendHeartbeat()
		if err != nil {
			panic(err)
		}

		config, err := adapter.GetActiveConfig()
		if err != nil {
			panic(err)
		}

		err = adapter.SendActiveConfig(config)
		if err != nil {
			panic(err)
		}

		conf, _ := adapter.GetProposedConfig()

		if conf != nil {
			err = adapter.ApplyConfig(conf)
			if err != nil {
				panic(err)
			}
		}
		time.Sleep(time.Second * 10)
	}

}

func main() {
	// Set the file name of the configurations file
	viper.SetConfigName("dbtune")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".") // optionally look for config in the working directory

	// Read the configuration file
	// Attempt to read the configuration file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore the error
			log.Println("No config file found, proceeding with environment variables only.")
		} else {
			// Config file was found but another error occurred
			log.Fatalf("Error reading config file, %s", err)
		}
	}

	viper.AutomaticEnv() // Read also environment variables, need this to work with Docker containers without mounting config file
	// Define environment variable prefix (optional)
	viper.SetEnvPrefix("DBT")                                                   // Set a prefix for environment variables
	viper.BindEnv("postgresql.connection_url", "DBT_POSTGRESQL_CONNECTION_URL") // Bind to the environment variable

	var pgAdapter = adapters.CreateDefaultPostgreSQLAdapter(
		"http://localhost:8000",
		"3071dd11-91e6-4351-b445-81a6f121d6ee",
		"920d7224-ebb3-4df3-94b7-78684b5efe35",
	)

	// runner(pgAdapter)

	fmt.Println(pgAdapter)
}
