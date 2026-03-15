package dbtune

import (
	"strings"
	"testing"

	"github.com/dbtuneai/agent/pkg/internal/utils"
)

func TestServerURLsValidation(t *testing.T) {
	validDbID := "550e8400-e29b-41d4-a716-446655440000"
	validApiKey := "a1b2c3d4-e5f6-7890-abcd-ef1234567890" //nolint:gosec // test value, not a real credential
	validURL := "https://app.dbtune.com"

	tests := []struct {
		name          string
		input         ServerURLs
		expectError   bool
		errorContains []string
	}{
		{
			name: "Valid config",
			input: ServerURLs{
				ServerUrl: validURL,
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError: false,
		},
		{
			name: "Invalid database_id - not a UUID",
			input: ServerURLs{
				ServerUrl: validURL,
				ApiKey:    validApiKey,
				DbID:      "not-a-uuid",
			},
			expectError:   true,
			errorContains: []string{"database_id"},
		},
		{
			name: "Invalid database_id - empty",
			input: ServerURLs{
				ServerUrl: validURL,
				ApiKey:    validApiKey,
				DbID:      "",
			},
			expectError:   true,
			errorContains: []string{"database_id"},
		},
		{
			name: "Invalid database_id - partial UUID",
			input: ServerURLs{
				ServerUrl: validURL,
				ApiKey:    validApiKey,
				DbID:      "550e8400-e29b-41d4",
			},
			expectError:   true,
			errorContains: []string{"database_id"},
		},
		{
			name: "Invalid api_key - not a UUID",
			input: ServerURLs{
				ServerUrl: validURL,
				ApiKey:    "not-a-uuid-key",
				DbID:      validDbID,
			},
			expectError:   true,
			errorContains: []string{"api_key"},
		},
		{
			name: "Invalid api_key - empty",
			input: ServerURLs{
				ServerUrl: validURL,
				ApiKey:    "",
				DbID:      validDbID,
			},
			expectError:   true,
			errorContains: []string{"api_key"},
		},
		{
			name: "Invalid server URL - not a URL",
			input: ServerURLs{
				ServerUrl: "not-a-url",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError:   true,
			errorContains: []string{"server_url"},
		},
		{
			name: "Invalid server URL - empty",
			input: ServerURLs{
				ServerUrl: "",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError:   true,
			errorContains: []string{"server_url"},
		},
		{
			name: "All fields missing",
			input: ServerURLs{
				ServerUrl: "",
				ApiKey:    "",
				DbID:      "",
			},
			expectError:   true,
			errorContains: []string{"server_url", "api_key", "database_id"},
		},
		{
			name: "Valid config with different UUIDs",
			input: ServerURLs{
				ServerUrl: "https://custom.dbtune.com",
				ApiKey:    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				DbID:      "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
			},
			expectError: false,
		},
		{
			name: "Valid server URL - localhost",
			input: ServerURLs{
				ServerUrl: "http://localhost:8080",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError: false,
		},
		{
			name: "Valid server URL - localhost no port",
			input: ServerURLs{
				ServerUrl: "http://localhost",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError: false,
		},
		{
			name: "Valid server URL - IP address with port",
			input: ServerURLs{
				ServerUrl: "http://192.168.1.100:3000",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError: false,
		},
		{
			name: "Valid server URL - HTTPS with path",
			input: ServerURLs{
				ServerUrl: "https://dbtune.example.com/api",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError: false,
		},
		{
			name: "Invalid server URL - missing scheme",
			input: ServerURLs{
				ServerUrl: "app.dbtune.com",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError:   true,
			errorContains: []string{"server_url"},
		},
		{
			name: "Invalid server URL - just a port",
			input: ServerURLs{
				ServerUrl: ":8080",
				ApiKey:    validApiKey,
				DbID:      validDbID,
			},
			expectError:   true,
			errorContains: []string{"server_url"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := utils.ValidateStruct(&tt.input)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got nil")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("expected no error but got: %v", err)
				return
			}

			if tt.expectError && err != nil {
				errStr := err.Error()
				for _, expected := range tt.errorContains {
					if !strings.Contains(errStr, expected) {
						t.Errorf("error message '%s' does not contain '%s'", errStr, expected)
					}
				}
			}
		})
	}
}

func TestServerURLsGeneration(t *testing.T) {
	s := ServerURLs{
		ServerUrl: "https://app.dbtune.com",
		ApiKey:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		DbID:      "550e8400-e29b-41d4-a716-446655440000",
	}

	tests := []struct {
		name     string
		method   func() string
		expected string
	}{
		{
			name:     "PostHeartbeat",
			method:   s.PostHeartbeat,
			expected: "https://app.dbtune.com/api/v1/agent/heartbeat?uuid=550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "PostSystemInfo",
			method:   s.PostSystemInfo,
			expected: "https://app.dbtune.com/api/v1/agent/system-info?uuid=550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "PostMetrics",
			method:   s.PostMetrics,
			expected: "https://app.dbtune.com/api/v1/agent/metrics?uuid=550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "PostActiveConfig",
			method:   s.PostActiveConfig,
			expected: "https://app.dbtune.com/api/v1/agent/configurations?uuid=550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "GetKnobRecommendations",
			method:   s.GetKnobRecommendations,
			expected: "https://app.dbtune.com/api/v1/agent/configurations?uuid=550e8400-e29b-41d4-a716-446655440000&status=recommended",
		},
		{
			name:     "PostGuardrailSignal",
			method:   s.PostGuardrailSignal,
			expected: "https://app.dbtune.com/api/v1/agent/guardrails?uuid=550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "PostError",
			method:   s.PostError,
			expected: "https://app.dbtune.com/api/v1/agent/log-entries?uuid=550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.method()
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}
