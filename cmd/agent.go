package main

import (
	"context"
	"flag"
	"log"
	"time"

	"example.com/dbtune-agent/internal/adapters"
	"example.com/dbtune-agent/internal/utils"
	"github.com/spf13/viper"
)

func runner(adapter utils.AgentLooper) {
	logger := adapter.Logger()

	// Create tickers for different intervals
	metricsTicker := time.NewTicker(5 * time.Second)
	//systemMetricsTicker := time.NewTicker(1 * time.Minute)
	systemMetricsTicker := time.NewTicker(5 * time.Second)
	configTicker := time.NewTicker(15 * time.Second)
	heartbeatTicker := time.NewTicker(15 * time.Second)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Heartbeat goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeatTicker.C:
				if err := adapter.SendHeartbeat(); err != nil {
					logger.Errorf("heartbeat sending error: %v", err)
				}
			}
		}
	}()

	// Metrics collection goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-metricsTicker.C:
				data, err := adapter.GetMetrics()
				if err != nil {
					logger.Errorf("metrics collection error: %v", err)
					continue
				}
				if err := adapter.SendMetrics(data); err != nil {
					logger.Errorf("metrics sending error: %v", err)
				}
			}
		}
	}()

	// System metrics collection goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-systemMetricsTicker.C:
				data, err := adapter.GetSystemInfo()
				if err != nil {
					logger.Errorf("system info collection error: %v", err)
					continue
				}
				if err := adapter.SendSystemInfo(data); err != nil {
					logger.Errorf("system info sending error: %v", err)
				}
			}
		}
	}()

	// Config management goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-configTicker.C:
				// Get and send current config
				config, err := adapter.GetActiveConfig()
				if err != nil {
					logger.Errorf("config collection error: %v", err)
					continue
				}
				if err := adapter.SendActiveConfig(config); err != nil {
					logger.Errorf("config sending error: %v", err)
					continue
				}

				// Check for and apply new config recommendations
				proposedConfig, err := adapter.GetProposedConfig()
				if err != nil {
					logger.Errorf("proposed config fetch error: %v", err)
					continue
				}

				if proposedConfig != nil {
					if err := adapter.ApplyConfig(proposedConfig); err != nil {
						logger.Errorf("config application error: %v", err)
					}
				}
			}
		}
	}()

	// Block forever
	select {}
}

func main() {
	// Define flags
	useDocker := flag.Bool("docker", false, "Use Docker adapter")
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

	var adapter utils.AgentLooper
	var err error

	// Create the appropriate adapter based on flags
	switch {
	case *useDocker:
		adapter, err = adapters.CreateDockerContainerAdapter()
		if err != nil {
			log.Fatalf("Failed to create Docker adapter: %v", err)
		}
	default:
		adapter, err = adapters.CreateDefaultPostgreSQLAdapter()
		if err != nil {
			log.Fatalf("Failed to create PostgreSQL adapter: %v", err)
		}
	}

	runner(adapter)
}
