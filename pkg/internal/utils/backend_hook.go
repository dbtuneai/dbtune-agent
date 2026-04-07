package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultFlushInterval = 15 * time.Second
	defaultBufferSize    = 100
	sendTimeout          = 5 * time.Second
)

type backendLogEntry struct {
	Message   string `json:"message"`
	Level     string `json:"level"`
	Timestamp string `json:"timestamp"`
}

// BackendHook is a logrus.Hook that captures warn/error/fatal/panic level logs
// and sends them to the DBtune backend for internal observability.
type BackendHook struct {
	client *http.Client
	url    string
	apiKey string

	entries  chan backendLogEntry
	flushReq chan chan struct{}
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

// CreateBackendHook creates a new BackendHook with default configuration.
func CreateBackendHook(url, apiKey string) *BackendHook {
	return CreateBackendHookWithConfig(url, apiKey, defaultFlushInterval, defaultBufferSize)
}

// CreateBackendHookWithConfig creates a new BackendHook with custom flush interval
// and channel buffer size.
func CreateBackendHookWithConfig(url, apiKey string, flushInterval time.Duration, bufferSize int) *BackendHook {
	h := &BackendHook{
		client:   &http.Client{Timeout: sendTimeout},
		url:      url,
		apiKey:   apiKey,
		entries:  make(chan backendLogEntry, bufferSize),
		flushReq: make(chan chan struct{}),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go h.flushLoop(flushInterval)
	return h
}

// Levels implements logrus.Hook. It returns the log levels that this hook
// should be fired for (warn, error, fatal, and panic).
func (h *BackendHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}

func (h *BackendHook) Fire(entry *logrus.Entry) error {
	caller := ""
	if entry.Caller != nil {
		caller = fmt.Sprintf("%s:%d", path.Base(entry.Caller.File), entry.Caller.Line)
	}

	message := entry.Message
	if caller != "" {
		message = fmt.Sprintf("[%s] %s", caller, message)
	}

	if len(entry.Data) > 0 {
		fieldsJSON, err := json.Marshal(entry.Data)
		if err == nil {
			message = fmt.Sprintf("%s | %s", message, string(fieldsJSON))
		}
	}

	le := backendLogEntry{
		Message:   message,
		Level:     entry.Level.String(),
		Timestamp: entry.Time.UTC().Format(time.RFC3339),
	}

	// Non-blocking send: drop the entry if the channel is full.
	select {
	case h.entries <- le:
	default:
	}

	if entry.Level == logrus.FatalLevel || entry.Level == logrus.PanicLevel {
		h.Flush()
	}

	return nil
}

// Close stops the flush loop and drains any remaining buffered entries.
// It is safe to call multiple times.
func (h *BackendHook) Close() {
	h.once.Do(func() {
		close(h.stop)
	})
	<-h.done
}

// Flush triggers an immediate send of all buffered entries.
func (h *BackendHook) Flush() {
	ack := make(chan struct{})
	select {
	case h.flushReq <- ack:
		<-ack
	case <-h.stop:
	}
}

func (h *BackendHook) flushLoop(flushInterval time.Duration) {
	defer close(h.done)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var buf []backendLogEntry

	for {
		select {
		case e := <-h.entries:
			buf = append(buf, e)

		case <-ticker.C:
			if len(buf) > 0 {
				h.sendBatch(buf)
				buf = buf[:0]
			}

		case ack := <-h.flushReq:
		drain:
			for {
				select {
				case e := <-h.entries:
					buf = append(buf, e)
				default:
					break drain
				}
			}
			if len(buf) > 0 {
				h.sendBatch(buf)
				buf = buf[:0]
			}
			close(ack)

		case <-h.stop:
			for {
				select {
				case e := <-h.entries:
					buf = append(buf, e)
				default:
					if len(buf) > 0 {
						h.sendBatch(buf)
					}
					return
				}
			}
		}
	}
}

func (h *BackendHook) sendBatch(entries []backendLogEntry) {
	jsonData, err := json.Marshal(entries)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", h.url, bytes.NewBuffer(jsonData))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DBTUNE-API-KEY", h.apiKey)

	resp, err := h.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}
