package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dbtuneai/agent/pkg/config"
	guardrails "github.com/dbtuneai/agent/pkg/guardrails"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/dbtuneai/agent/pkg/tuning"
)

// SendHeartbeat sends a heartbeat to the DBtune server
// to indicate that the agent is running.
// This method does not need to be overridden by any adapter
func (c *Client) SendHeartbeat(ctx context.Context) error {
	url := c.ServerURLs.AgentURL("heartbeat")
	c.Logger().Infof("Sending heartbeat to %s", url)

	payload := AgentPayload{
		AgentVersion:   c.Version,
		AgentStartTime: c.StartTime,
		AgentID:        c.AgentID,
		AllowRestart:   config.IsRestartAllowed(),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.Logger().Errorf("Error marshaling JSON: %s", err)
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	resp, err := c.postJSON(ctx, url, jsonData)
	if err != nil {
		c.Logger().Errorf("Failed to send heartbeat: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		c.Logger().Errorf("Failed to send heartbeat to %s, status: %d, body: %s", url, resp.StatusCode, string(body))
		return fmt.Errorf("heartbeat failed with status code %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) SendMetrics(ctx context.Context, ms []metrics.FlatValue) error {
	c.Logger().Println("Sending metrics to server")

	formattedMetrics := FormatMetrics(ms)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	resp, err := c.postJSON(ctx, c.ServerURLs.AgentURL("metrics"), jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		c.Logger().Warnf("Failed to send metrics. Response body: %s", string(body))
		return fmt.Errorf("failed to send metrics, code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) SendSystemInfo(ctx context.Context, systemInfo []metrics.FlatValue) error {
	c.Logger().Println("Sending system info to server")

	formattedMetrics := FormatSystemInfo(systemInfo)

	jsonData, err := json.Marshal(formattedMetrics)
	if err != nil {
		return err
	}

	resp, err := c.putJSON(ctx, c.ServerURLs.AgentURL("system-info"), jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		c.Logger().Errorf("Failed to send system info. Response body: %s", string(body))
		return fmt.Errorf("failed to send system info, code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) SendActiveConfig(ctx context.Context, config tuning.ConfigArraySchema, observedAt time.Time) error {
	c.Logger().Println("Sending active configuration to server")

	type Payload struct {
		Config     tuning.ConfigArraySchema `json:"config"`
		Hash       string                   `json:"hash"`
		ObservedAt string                   `json:"observed_at"`
	}

	hash, _ := HashJSON(config)

	jsonData, err := json.Marshal(Payload{
		Config:     config,
		Hash:       hash,
		ObservedAt: observedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}

	c.Logger().Debugf("Active config payload: %s", string(jsonData))

	resp, err := c.postJSONGzip(ctx, c.ServerURLs.AgentURL("configurations"), jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		c.Logger().Errorf("Failed to send configuration info. Response body: %s", string(body))
		return fmt.Errorf("failed to send config info, code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) GetProposedConfig(ctx context.Context) (*tuning.ProposedConfigResponse, error) {
	c.Logger().Println("Fetching proposed configurations")

	resp, err := c.getRequest(ctx, c.ServerURLs.GetKnobRecommendations())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var proposedConfig []tuning.ProposedConfigResponse

	if err := json.Unmarshal(body, &proposedConfig); err != nil {
		return nil, err
	}

	if len(proposedConfig) > 0 {
		return &proposedConfig[0], nil
	}

	return nil, nil
}

// SendGuardrailSignal sends a guardrail signal to the DBtune server
// that something is heading towards a failure.
func (c *Client) SendGuardrailSignal(ctx context.Context, signal guardrails.Signal) error {
	c.Logger().Warnf("🚨 Sending Guardrail, level: %s, type: %s", signal.Level, signal.Type)

	jsonData, err := json.Marshal(signal)
	if err != nil {
		return err
	}

	resp, err := c.postJSON(ctx, c.ServerURLs.AgentURL("guardrails"), jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		c.Logger().Error("Failed to send guardrail signal. Response body: ", string(body))
		return fmt.Errorf("failed to send guardrail signal, code: %d", resp.StatusCode)
	}

	return nil
}

// catalogSendTimeout bounds a single SendCatalogPayload POST, including any
// retries performed by the retryable http client.
const catalogSendTimeout = 30 * time.Second

// SendCatalogPayload sends pre-marshaled JSON bytes for a catalog view,
// gzip-compressed, to the DBtune server.
func (c *Client) SendCatalogPayload(ctx context.Context, name string, payload []byte) error {
	c.Logger().Printf("Sending %s payload (%d bytes)", name, len(payload))

	ctx, cancel := context.WithTimeout(ctx, catalogSendTimeout)
	defer cancel()

	resp, err := c.postJSONGzip(ctx, c.ServerURLs.AgentURL(name), payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("failed to send %s, code: %d, body: %s", name, resp.StatusCode, body)
	}
	return nil
}

func (c *Client) SendError(ctx context.Context, payload ErrorPayload) error {
	c.Logger().Errorf("🚨 Sending Error Report, type: %s, message: %s", payload.ErrorType, payload.ErrorMessage)

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := c.postJSON(ctx, c.ServerURLs.AgentURL("log-entries"), jsonData)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		c.Logger().Errorf("Failed to send error. Response body: %s", string(body))
		return fmt.Errorf("failed to send error, code: %d", resp.StatusCode)
	}

	return nil
}
