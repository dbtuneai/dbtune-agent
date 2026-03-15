package queries

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnsureTimeout(t *testing.T) {
	t.Run("adds timeout when context has no deadline", func(t *testing.T) {
		ctx := context.Background()
		newCtx, cancel := EnsureTimeout(ctx)
		defer cancel()

		deadline, ok := newCtx.Deadline()
		assert.True(t, ok, "should have a deadline")
		// Deadline should be roughly DefaultQueryTimeout from now
		assert.WithinDuration(t, time.Now().Add(DefaultQueryTimeout), deadline, 1*time.Second)
	})

	t.Run("preserves existing deadline when shorter", func(t *testing.T) {
		shortTimeout := 5 * time.Second
		ctx, outerCancel := context.WithTimeout(context.Background(), shortTimeout)
		defer outerCancel()

		newCtx, cancel := EnsureTimeout(ctx)
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

		newCtx, cancel := EnsureTimeout(ctx)
		defer cancel()

		deadline, ok := newCtx.Deadline()
		assert.True(t, ok, "should have a deadline")
		// Should preserve the original longer deadline (EnsureTimeout doesn't override)
		assert.WithinDuration(t, time.Now().Add(longTimeout), deadline, 1*time.Second)
	})
}
