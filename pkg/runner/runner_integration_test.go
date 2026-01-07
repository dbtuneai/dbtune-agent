package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/router"
	"github.com/dbtuneai/agent/pkg/sink"
	"github.com/dbtuneai/agent/pkg/source"
	"github.com/hashicorp/go-retryablehttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func TestFileSinkIntegration(t *testing.T) {
	// Clean up any existing debug.log
	os.Remove("debug.log")
	defer os.Remove("debug.log")

	// Set debug mode
	viper.Set("debug", true)
	defer viper.Set("debug", false)

	// Create a minimal CommonAgent
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	client := retryablehttp.NewClient()
	client.RetryMax = 0 // Don't retry in tests

	serverURL := dbtune.ServerURLs{
		ServerUrl: "https://test.example.com",
		ApiKey:    "test-key",
		DbID:      "test-db",
	}

	commonAgent := &agent.CommonAgent{
		ServerURLs: serverURL,
		APIClient:  client,
		StartTime:  time.Now().UTC().Format(time.RFC3339),
		Version:    "test-1.0.0",
		MetricsState: agent.MetricsState{
			Collectors: []agent.MetricCollector{
				{
					Key: "test_collector",
					Collector: func(ctx context.Context, state *agent.MetricsState) error {
						metric, _ := metrics.NewMetric("test_metric", int64(42), metrics.Int)
						state.AddMetric(metric)
						return nil
					},
				},
			},
			Cache:   agent.Caches{},
			Metrics: []metrics.FlatValue{},
			Mutex:   &sync.Mutex{},
		},
	}

	// Run the runner in a goroutine and cancel after a short time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		// Create sources manually to avoid complex setup
		sources := []source.Source{
			// Just use heartbeat as a simple test
			source.NewHeartbeatSource(
				commonAgent.Version,
				commonAgent.StartTime,
				100*time.Millisecond,
				logger,
			),
		}

		// Manually create router with both sinks
		sinks := make([]sink.Sink, 0)

		// Add platform sink
		platformSink := sink.NewDBTunePlatformSink(client, serverURL, logger)
		sinks = append(sinks, platformSink)

		// Add file sink
		fileSink, _ := sink.NewFileSink("debug.log", logger)
		sinks = append(sinks, fileSink)

		config := router.Config{
			BufferSize:    1000,
			FlushInterval: 5 * time.Second,
		}
		r := router.New(sources, sinks, logger, config)
		r.Run(ctx)
	}()

	// Wait for runner to run and generate some events
	time.Sleep(300 * time.Millisecond)
	cancel() // Stop the runner

	// Give it a moment to flush and close
	time.Sleep(100 * time.Millisecond)

	// Verify debug.log was created and contains events
	file, err := os.Open("debug.log")
	if err != nil {
		t.Fatalf("debug.log was not created: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	hasHeartbeat := false

	for scanner.Scan() {
		lineCount++
		line := scanner.Text()

		// Parse JSON
		var eventData map[string]interface{}
		if err := json.Unmarshal([]byte(line), &eventData); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", lineCount, err)
			continue
		}

		// Check for heartbeat event
		if eventData["type"] == "heartbeat" {
			hasHeartbeat = true
		}
	}

	if lineCount == 0 {
		t.Error("debug.log is empty - no events were written")
	}

	if !hasHeartbeat {
		t.Error("expected at least one heartbeat event in debug.log")
	}

	t.Logf("Successfully wrote %d events to debug.log", lineCount)
}

// mockAgentLooper is a minimal implementation for testing
type mockAgentLooper struct {
	*agent.CommonAgent
}

func (m *mockAgentLooper) GetSystemInfo() ([]metrics.FlatValue, error) {
	return []metrics.FlatValue{}, nil
}

func (m *mockAgentLooper) GetActiveConfig() (agent.ConfigArraySchema, error) {
	return agent.ConfigArraySchema{}, nil
}

func (m *mockAgentLooper) GetProposedConfig() (*agent.ProposedConfigResponse, error) {
	return nil, nil
}

func (m *mockAgentLooper) ApplyConfig(knobs *agent.ProposedConfigResponse) error {
	return nil
}

func (m *mockAgentLooper) Guardrails() *guardrails.Signal {
	return nil
}
