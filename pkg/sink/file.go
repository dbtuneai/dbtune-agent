package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dbtuneai/agent/pkg/events"
	log "github.com/sirupsen/logrus"
)

// FileSink writes events to a file in JSONL format (one JSON object per line)
type FileSink struct {
	file   *os.File
	writer *bufio.Writer
	logger *log.Logger
	mu     sync.Mutex
}

// NewFileSink creates a new file sink that writes to the specified path
func NewFileSink(path string, logger *log.Logger) (*FileSink, error) {
	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	return &FileSink{
		file:   file,
		writer: bufio.NewWriter(file),
		logger: logger,
	}, nil
}

// Name returns the sink name
func (s *FileSink) Name() string {
	return "file"
}

// Process handles an incoming event by writing it to the file
func (s *FileSink) Process(ctx context.Context, event events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a serializable representation of the event
	var eventData map[string]interface{}

	switch e := event.(type) {
	case events.MetricsEvent:
		eventData = map[string]interface{}{
			"type":          string(e.Type()),
			"timestamp":     e.Timestamp().Format(time.RFC3339Nano),
			"collector_key": e.CollectorKey(),
			"metrics":       e.Metrics(),
		}
	case events.HeartbeatEvent:
		eventData = map[string]interface{}{
			"type":       string(e.Type()),
			"timestamp":  e.Timestamp().Format(time.RFC3339Nano),
			"version":    e.Version(),
			"start_time": e.StartTime(),
		}
	case events.SystemInfoEvent:
		eventData = map[string]interface{}{
			"type":      string(e.Type()),
			"timestamp": e.Timestamp().Format(time.RFC3339Nano),
			"info":      e.Info(),
		}
	case events.ConfigEvent:
		eventData = map[string]interface{}{
			"type":            string(e.Type()),
			"timestamp":       e.Timestamp().Format(time.RFC3339Nano),
			"active_config":   e.ActiveConfig(),
			"proposed_config": e.ProposedConfig(),
		}
	case events.GuardrailEvent:
		eventData = map[string]interface{}{
			"type":      string(e.Type()),
			"timestamp": e.Timestamp().Format(time.RFC3339Nano),
			"signal":    e.Signal(),
		}
	case events.ErrorEvent:
		eventData = map[string]interface{}{
			"type":      string(e.Type()),
			"timestamp": e.Timestamp().Format(time.RFC3339Nano),
			"payload":   e.Payload(),
		}
	default:
		s.logger.Warnf("[file] unknown event type: %v", event.Type())
		return nil
	}

	// Serialize to JSON
	data, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Write to file with newline
	if _, err := s.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}
	if err := s.writer.WriteByte('\n'); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

// Flush flushes the buffered writer
func (s *FileSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writer.Flush()
}

// FlushMetrics is a no-op for file sink (metrics are written immediately)
// This implements the Flusher interface for compatibility with the router
func (s *FileSink) FlushMetrics(ctx context.Context) error {
	return s.Flush()
}

// Close flushes and closes the file
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		s.logger.Errorf("Failed to flush file writer: %v", err)
	}

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
