package queries

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeSettingName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple name", "shared_buffers", false},
		{"valid dotted name", "pg_stat_statements.max", false},
		{"valid with numbers", "wal_level2", false},
		{"sql injection semicolon", "shared_buffers; DROP TABLE users", true},
		{"sql injection quote", "shared_buffers' OR '1'='1", true},
		{"sql injection comment", "shared_buffers -- comment", true},
		{"sql injection newline", "shared_buffers\nDROP TABLE users", true},
		{"empty string", "", true},
		{"parentheses", "shared_buffers()", true},
		{"equals sign", "shared_buffers=1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SanitizeSettingName(tt.input)
			if tt.wantErr {
				assert.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				assert.NoError(t, err, "unexpected error for input: %s", tt.input)
			}
		})
	}
}

func TestSanitizeSettingValue(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"numeric value", "256", "'256'", false},
		{"value with unit", "256MB", "'256MB'", false},
		{"string value", "on", "'on'", false},
		{"value with dot", "0.9", "'0.9'", false},
		{"sql injection via quote", "256'; DROP TABLE users; --", "", true},
		{"sql injection via semicolon", "256; DROP TABLE users", "", true},
		{"sql injection via double dash", "256 -- comment", "", true},
		{"empty value", "", "''", false},
		{"value with backslash", "some\\value", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizeSettingValue(tt.input)
			if tt.wantErr {
				assert.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				assert.NoError(t, err, "unexpected error for input: %s", tt.input)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSanitizeDatabaseName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"simple name", "mydb", `"mydb"`, false},
		{"name with underscore", "my_db", `"my_db"`, false},
		{"sql injection semicolon", "mydb; DROP TABLE users", "", true},
		{"sql injection quote", `mydb" ; DROP TABLE users; --`, "", true},
		{"empty name", "", "", true},
		{"name with dash", "my-db", `"my-db"`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizeDatabaseName(tt.input)
			if tt.wantErr {
				assert.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				assert.NoError(t, err, "unexpected error for input: %s", tt.input)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
