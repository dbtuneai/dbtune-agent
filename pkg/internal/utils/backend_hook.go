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
	flushInterval = 10 * time.Second
	maxBufferSize = 100
	sendTimeout   = 5 * time.Second
)

type backendLogEntry struct {
	Message   string `json:"message"`
	Level     string `json:"level"`
	Timestamp string `json:"timestamp"`
}

// BackendHook is a logrus hook that captures warn/error level logs
// and sends them to the DBtune backend for internal observability.
type BackendHook struct {
	client *http.Client
	url    string
	apiKey string

	mu      sync.Mutex
	buffer  []backendLogEntry
	sending bool
}

func CreateBackendHook(url, apiKey string) *BackendHook {
	h := &BackendHook{
		client: &http.Client{Timeout: sendTimeout},
		url:    url,
		apiKey: apiKey,
		buffer: make([]backendLogEntry, 0, maxBufferSize),
	}
	go h.flushLoop()
	return h
}

func (h *BackendHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}

func (h *BackendHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()

	// Prevent recursion: HTTP calls during flush may trigger logs
	if h.sending {
		h.mu.Unlock()
		return nil
	}

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

	if len(h.buffer) >= maxBufferSize {
		h.buffer = h.buffer[1:]
	}

	h.buffer = append(h.buffer, backendLogEntry{
		Message:   message,
		Level:     entry.Level.String(),
		Timestamp: entry.Time.UTC().Format(time.RFC3339),
	})

	needsSync := entry.Level == logrus.FatalLevel || entry.Level == logrus.PanicLevel
	h.mu.Unlock()

	if needsSync {
		h.Flush()
	}

	return nil
}

func (h *BackendHook) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		h.Flush()
	}
}

func (h *BackendHook) Flush() {
	h.mu.Lock()
	if len(h.buffer) == 0 || h.sending {
		h.mu.Unlock()
		return
	}

	entries := h.buffer
	h.buffer = make([]backendLogEntry, 0, maxBufferSize)
	h.sending = true
	h.mu.Unlock()

	h.sendBatch(entries)

	h.mu.Lock()
	h.sending = false
	h.mu.Unlock()
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
