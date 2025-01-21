package main

import (
	"context"
	"flag"
	"log"
	"time"

	"example.com/dbtune-agent/internal/adapters"
	"example.com/dbtune-agent/internal/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

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

func runWithTicker(ctx context.Context, ticker *time.Ticker, name string, logger *logrus.Logger, fn func() error) {
	// Run immediately
	if err := fn(); err != nil {
		logger.Errorf("initial %s error: %v", name, err)
	}

	// Then run on ticker
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fn(); err != nil {
				logger.Errorf("%s error: %v", name, err)
			}
		}
	}
}

func runner(adapter utils.AgentLooper) {
	logger := adapter.Logger()

	// Create tickers for different intervals
	metricsTicker := time.NewTicker(5 * time.Second)
	systemMetricsTicker := time.NewTicker(1 * time.Minute)
	configTicker := time.NewTicker(5 * time.Second)
	heartbeatTicker := time.NewTicker(15 * time.Second)
	guardrailTicker := time.NewTicker(1 * time.Second)

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Heartbeat goroutine
	go runWithTicker(ctx, heartbeatTicker, "heartbeat", logger, adapter.SendHeartbeat)

	// Metrics collection goroutine
	go runWithTicker(ctx, metricsTicker, "metrics", logger, func() error {
		data, err := adapter.GetMetrics()
		if err != nil {
			return err
		}
		return adapter.SendMetrics(data)
	})

	// System metrics collection goroutine
	go runWithTicker(ctx, systemMetricsTicker, "system info", logger, func() error {
		data, err := adapter.GetSystemInfo()
		if err != nil {
			return err
		}
		return adapter.SendSystemInfo(data)
	})

	// Config management goroutine
	go runWithTicker(ctx, configTicker, "config", logger, func() error {
		config, err := adapter.GetActiveConfig()
		if err != nil {
			return err
		}
		if err := adapter.SendActiveConfig(config); err != nil {
			return err
		}

		proposedConfig, err := adapter.GetProposedConfig()
		if err != nil {
			return err
		}
		if proposedConfig != nil {
			return adapter.ApplyConfig(proposedConfig)
		}
		return nil
	})

	// Guardrail check goroutine
	// Time is kept in a pointer to keep a persistent reference
	// May need to refactor this for testing
	var lastCheck *time.Time
	go runWithTicker(ctx, guardrailTicker, "guardrail", logger, func() error {
		if lastCheck != nil && time.Since(*lastCheck) < 15*time.Second {
			return nil
		}
		if level := adapter.Guardrails(); level != nil {
			if err := adapter.SendGuardrailSignal(*level); err != nil {
				now := time.Now()
				lastCheck = &now
				return err
			}
			now := time.Now()
			lastCheck = &now
		}
		return nil
	})

	// Block forever
	select {}
}
