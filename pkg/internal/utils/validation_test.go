package utils

import (
	"strings"
	"testing"
)

// ConfigStruct represents a typical configuration struct with validation tags
type ConfigStruct struct {
	Host     string `validate:"required" mapstructure:"host"`
	Port     string `validate:"required" mapstructure:"port"`
	Username string `validate:"required" mapstructure:"username"`
	Password string `mapstructure:"password"`
}

func TestValidateStruct(t *testing.T) {
	tests := []struct {
		name          string
		input         interface{}
		expectError   bool
		errorContains []string
	}{
		{
			name: "Valid config",
			input: &ConfigStruct{
				Host:     "localhost",
				Port:     "5432",
				Username: "admin",
				Password: "secret",
			},
			expectError: false,
		},
		{
			name: "Missing required fields",
			input: &ConfigStruct{
				Host: "localhost",
				// Port and Username missing
			},
			expectError:   true,
			errorContains: []string{"port is required", "username is required"},
		},
		{
			name: "Missing one required field",
			input: &ConfigStruct{
				Host: "localhost",
				Port: "5432",
				// Username missing
			},
			expectError:   true,
			errorContains: []string{"username is required"},
		},
		{
			name:          "Nil input",
			input:         nil,
			expectError:   true,
			errorContains: []string{"Invalid validation"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStruct(tt.input)

			// Check if error was expected
			if tt.expectError && err == nil {
				t.Errorf("expected error but got nil")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("expected no error but got: %v", err)
				return
			}

			// If error was expected, check error message contains expected strings
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
