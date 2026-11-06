// Package backend is the agent's client for the DBtune API: the HTTP client and
// credentials, one method per endpoint, the payload formats, and the log hook
// that ships warnings and errors to the backend.
package backend

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
	log "github.com/sirupsen/logrus"
)

// Client is the agent's connection to the DBtune backend: the server URLs and
// API key, the retrying HTTP client, and the identity sent with heartbeats.
type Client struct {
	ServerURLs
	logger    *log.Logger
	APIClient *retryablehttp.Client
	// Unique identifier for this agent instance
	AgentID string
	// Time the agent started
	StartTime string
	// Version information
	Version string
}

// NewClient reads the dbtune server settings and builds the HTTP client. It
// also attaches the hook that ships warn and error logs to the backend to
// logger. A missing or invalid server setting is fatal.
func NewClient(logger *log.Logger, version string) *Client {
	serverUrl, err := CreateServerURLs()
	if err != nil {
		logger.Fatalf("Error creating server URLs: %s", err)
	}

	client := retryablehttp.NewClient()
	// 30 retries, as the cap is 30seconds for the back-off wait time
	client.RetryMax = 30
	client.HTTPClient.Timeout = 4 * time.Second
	// retryablehttp logs every failed attempt at Error level. Those are
	// transient and retried, so silencing its internal logger keeps retry noise
	// off the backend log hook. The real outcome surfaces via the error Do
	// returns once retries are exhausted, logged by the caller (runWithTicker /
	// SendHeartbeat). Do guards all internal logging on a nil logger.
	client.Logger = nil

	// Intercept each attempt to add the API token and trace retries at Debug.
	client.RequestLogHook = func(_ retryablehttp.Logger, req *http.Request, attempt int) {
		key := req.Header.Get("DBTUNE-API-KEY")
		if key == "" {
			req.Header.Add("DBTUNE-API-KEY", serverUrl.ApiKey)
		}
		if attempt > 0 {
			logger.Debugf("retrying %s %s (attempt %d)", req.Method, req.URL, attempt+1)
		}
	}

	hook := CreateBackendHook(serverUrl.AgentURL("raw-logs"), serverUrl.ApiKey)
	logger.AddHook(hook)

	agentID := GetAgentID()
	logger.Infof("Agent instance ID: %s", agentID)

	return &Client{
		ServerURLs: serverUrl,
		APIClient:  client,
		logger:     logger,
		AgentID:    agentID,
		StartTime:  time.Now().UTC().Format(time.RFC3339),
		Version:    version,
	}
}

// Package-level agent ID, generated once and shared across all CommonAgent instances
var (
	agentIDOnce   sync.Once
	sharedAgentID string
)

// GetAgentID returns the shared agent ID, generating it once on first call
func GetAgentID() string {
	agentIDOnce.Do(func() {
		sharedAgentID = uuid.New().String()
	})
	return sharedAgentID
}

func (c *Client) Logger() *log.Logger {
	return c.logger
}

func (c *Client) WithLogger(logger *log.Logger) {
	c.logger = logger
}

// postJSON sends a POST request with JSON body using the provided context.
func (c *Client) postJSON(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.APIClient.Do(req)
}

// putJSON sends a PUT request with JSON body using the provided context.
func (c *Client) putJSON(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.APIClient.Do(req)
}

// postJSONGzip sends a POST request with a gzip-compressed JSON body
// using the provided context.
func (c *Client) postJSONGzip(ctx context.Context, url string, jsonData []byte) (*http.Response, error) {
	buf, err := gzipCompress(jsonData)
	if err != nil {
		return nil, err
	}
	c.Logger().Debugf("POST %s: %d bytes -> %d bytes gzipped", url, len(jsonData), buf.Len())
	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", url, buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	return c.APIClient.Do(req)
}

// getRequest sends a GET request using the provided context.
func (c *Client) getRequest(ctx context.Context, url string) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.APIClient.Do(req)
}

// gzipCompress compresses data using gzip and returns the compressed buffer.
func gzipCompress(data []byte) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	if _, err := gz.Write(data); err != nil {
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return buf, nil
}
