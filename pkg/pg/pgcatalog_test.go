package pg

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParsePgMajorVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"standard version", "16.2", 16},
		{"major only", "17", 17},
		{"three part version", "14.11.1", 14},
		{"pg 13", "13.4", 13},
		{"pg 18", "18.0", 18},
		{"empty string", "", 0},
		{"non-numeric", "abc.def", 0},
		{"non-numeric major", "abc", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParsePgMajorVersion(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnsureTimeout(t *testing.T) {
	t.Run("adds timeout when context has no deadline", func(t *testing.T) {
		ctx := context.Background()
		newCtx, cancel := ensureTimeout(ctx)
		defer cancel()

		deadline, ok := newCtx.Deadline()
		assert.True(t, ok, "should have a deadline")
		// Deadline should be roughly DefaultCatalogQueryTimeout from now
		assert.WithinDuration(t, time.Now().Add(DefaultCatalogQueryTimeout), deadline, 1*time.Second)
	})

	t.Run("preserves existing deadline when shorter", func(t *testing.T) {
		shortTimeout := 5 * time.Second
		ctx, outerCancel := context.WithTimeout(context.Background(), shortTimeout)
		defer outerCancel()

		newCtx, cancel := ensureTimeout(ctx)
		defer cancel()

		deadline, ok := newCtx.Deadline()
		assert.True(t, ok, "should have a deadline")
		// Should preserve the original shorter deadline
		assert.WithinDuration(t, time.Now().Add(shortTimeout), deadline, 1*time.Second)
	})

	t.Run("preserves existing deadline when longer", func(t *testing.T) {
		longTimeout := 5 * time.Minute
		ctx, outerCancel := context.WithTimeout(context.Background(), longTimeout)
		defer outerCancel()

		newCtx, cancel := ensureTimeout(ctx)
		defer cancel()

		deadline, ok := newCtx.Deadline()
		assert.True(t, ok, "should have a deadline")
		// Should preserve the original longer deadline (ensureTimeout doesn't override)
		assert.WithinDuration(t, time.Now().Add(longTimeout), deadline, 1*time.Second)
	})
}
