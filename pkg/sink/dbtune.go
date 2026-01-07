package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/dbtuneai/agent/pkg/events"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/hashicorp/go-retryablehttp"
	log "github.com/sirupsen/logrus"
)

// DBTunePlatformSink sends events to the dbtune platform
type DBTunePlatformSink struct {
	client    *retryablehttp.Client
	serverURL dbtune.ServerURLs
	logger    *log.Logger

	// Aggregation for metrics events
	metricsBuffer []metrics.FlatValue
	bufferMu      sync.Mutex
}

// NewDBTunePlatformSink creates a new DBTune platform sink
func NewDBTunePlatformSink(
	client *retryablehttp.Client,
	serverURL dbtune.ServerURLs,
	logger *log.Logger,
) *DBTunePlatformSink {
	return &DBTunePlatformSink{
		client:        client,
		serverURL:     serverURL,
		logger:        logger,
		metricsBuffer: make([]metrics.FlatValue, 0),
	}
}

// Name returns the sink name
func (s *DBTunePlatformSink) Name() string {
	return "dbtune-platform"
}

// Process handles an incoming event
func (s *DBTunePlatformSink) Process(ctx context.Context, event events.Event) error {
	switch e := event.(type) {
	case events.MetricsEvent:
		return s.handleMetrics(ctx, e)
	case events.HeartbeatEvent:
		return s.sendHeartbeat(ctx, e)
	case events.SystemInfoEvent:
		return s.sendSystemInfo(ctx, e)
	case events.ConfigEvent:
		return s.sendConfig(ctx, e)
	case events.GuardrailEvent:
		return s.sendGuardrail(ctx, e)
	case events.ErrorEvent:
		return s.sendError(ctx, e)
	default:
		s.logger.Warnf("[dbtune-platform] unknown event type: %v", event.Type())
	}
	return nil
}

// handleMetrics aggregates metrics for later flushing
func (s *DBTunePlatformSink) handleMetrics(ctx context.Context, event events.MetricsEvent) error {
	s.bufferMu.Lock()
	defer s.bufferMu.Unlock()

	s.metricsBuffer = append(s.metricsBuffer, event.Metrics...)
	return nil
}

// FlushMetrics sends all buffered metrics to platform
func (s *DBTunePlatformSink) FlushMetrics(ctx context.Context) error {
	s.bufferMu.Lock()
	if len(s.metricsBuffer) == 0 {
		s.bufferMu.Unlock()
		return nil
	}

	metricsToSend := s.metricsBuffer
	s.metricsBuffer = make([]metrics.FlatValue, 0)
	s.bufferMu.Unlock()

	s.logger.Info("Sending metrics to server")

	// NOTE(eddie): Even if the metrics were collected at slightly different times,
	// we'd like them to align on timestamp for better storage.
	// In practice, this alignment is non-problematic as we are not polling at
	// fine-grained intervals. If this were ever to change, we would have to handle
	// storage of metrics differently
	timestampForMetrics := time.Now()
	formatted := metrics.FormatMetrics(metricsToSend, timestampForMetrics)
	jsonData, err := json.Marshal(formatted)
	if err != nil {
		return err
	}

	// Add a timeout context to avoid hanging
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(reqCtx, "POST", s.serverURL.PostMetrics(), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Warnf("Failed to send metrics. Response body: %s", string(body))
		return fmt.Errorf("failed to send metrics, code: %d", resp.StatusCode)
	}

	return nil
}

// sendHeartbeat sends a heartbeat event to the platform
func (s *DBTunePlatformSink) sendHeartbeat(ctx context.Context, event events.HeartbeatEvent) error {
	s.logger.Infof("Sending heartbeat to %s", s.serverURL.PostHeartbeat())

	type payload struct {
		AgentVersion   string `json:"agent_version"`
		AgentStartTime string `json:"agent_start_time"`
	}

	p := payload{
		AgentVersion:   event.Version,
		AgentStartTime: event.StartTime,
	}

	jsonData, err := json.Marshal(p)
	if err != nil {
		s.logger.Errorf("Error marshaling JSON: %s", err)
		return err
	}

	// Add a timeout context to avoid hanging
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(reqCtx, "POST", s.serverURL.PostHeartbeat(), bytes.NewBuffer(jsonData))
	if err != nil {
		s.logger.Errorf("Failed to create heartbeat request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Errorf("Failed to send heartbeat: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Errorf("Failed to send heartbeat to %s, status: %d, body: %s", s.serverURL.PostHeartbeat(), resp.StatusCode, string(body))
		return fmt.Errorf("heartbeat failed with status code %d", resp.StatusCode)
	}

	return nil
}

// sendSystemInfo sends system info to the platform
func (s *DBTunePlatformSink) sendSystemInfo(ctx context.Context, event events.SystemInfoEvent) error {
	s.logger.Println("Sending system info to server")

	formatted := metrics.FormatSystemInfo(event.Info)
	jsonData, err := json.Marshal(formatted)
	if err != nil {
		return err
	}

	// Add a timeout context to avoid hanging
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(reqCtx, "PUT", s.serverURL.PostSystemInfo(), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Errorf("Failed to send system info. Response body: %s", string(body))
		return fmt.Errorf("failed to send system info, code: %d", resp.StatusCode)
	}

	return nil
}

// sendConfig sends configuration to the platform
func (s *DBTunePlatformSink) sendConfig(ctx context.Context, event events.ConfigEvent) error {
	s.logger.Println("Sending active configuration to server")

	type payload struct {
		Config any `json:"config"`
	}

	p := payload{Config: event.ActiveConfig}
	jsonData, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// Add a timeout context to avoid hanging
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(reqCtx, "POST", s.serverURL.PostActiveConfig(), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Errorf("Failed to send configuration info. Response body: %s", string(body))
		return fmt.Errorf("failed to send config info, code: %d", resp.StatusCode)
	}

	return nil
}

// sendGuardrail sends guardrail signal to the platform
func (s *DBTunePlatformSink) sendGuardrail(ctx context.Context, event events.GuardrailEvent) error {
	if event.Signal == nil {
		return nil // No signal to send
	}

	s.logger.Warnf("🚨 Sending Guardrail, level: %s, type: %s", event.Signal.Level, event.Signal.Type)

	jsonData, err := json.Marshal(event.Signal)
	if err != nil {
		return err
	}

	// Add a timeout context to avoid hanging
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(reqCtx, "POST", s.serverURL.PostGuardrailSignal(), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Error("Failed to send guardrail signal. Response body: ", string(body))
		return fmt.Errorf("failed to send guardrail signal, code: %d", resp.StatusCode)
	}

	return nil
}

// sendError sends error to the platform
func (s *DBTunePlatformSink) sendError(ctx context.Context, event events.ErrorEvent) error {
	s.logger.Errorf("🚨 Sending Error Report, type: %s, message: %s", event.Payload.ErrorType, event.Payload.ErrorMessage)

	jsonData, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}

	// Add a timeout context to avoid hanging
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := retryablehttp.NewRequestWithContext(reqCtx, "POST", s.serverURL.PostError(), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		s.logger.Errorf("Failed to send error. Response body: %s", string(body))
		return fmt.Errorf("failed to send error, code: %d", resp.StatusCode)
	}

	return nil
}

// Close flushes any remaining metrics
func (s *DBTunePlatformSink) Close() error {
	return s.FlushMetrics(context.Background())
}
