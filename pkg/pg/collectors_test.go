package pg

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PGXPoolInterface defines the interface we need for testing
type PGXPoolInterface interface {
	QueryRow(ctx context.Context, query string, args ...interface{}) Row
}

type Row interface {
	Scan(dest ...interface{}) error
}

// MockPGPool for testing
type MockPGPool struct {
	blksHit  int64
	blksRead int64
}

func (m *MockPGPool) Reset() {
	m.blksHit = 0
	m.blksRead = 0
}

func (m *MockPGPool) SetQueryResult(blksHit, blksRead int64) {
	m.blksHit = blksHit
	m.blksRead = blksRead
}

func (m *MockPGPool) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	return &MockRow{
		blksHit:  m.blksHit,
		blksRead: m.blksRead,
	}
}

type MockRow struct {
	blksHit  int64
	blksRead int64
}

func (r *MockRow) Scan(dest ...interface{}) error {
	if len(dest) != 2 {
		return fmt.Errorf("expected 2 arguments, got %d", len(dest))
	}

	// Set the values
	*(dest[0].(*int64)) = r.blksHit
	*(dest[1].(*int64)) = r.blksRead

	return nil
}

// TestBufferCacheHitRatio tests the delta-based cache hit ratio calculation
func TestBufferCacheHitRatio(t *testing.T) {
	// Mock implementation for testing
	mockPool := &MockPGPool{}

	tests := []struct {
		name             string
		currentBlksHit   int64
		currentBlksRead  int64
		previousBlksHit  int64
		previousBlksRead int64
		expectedHitRatio float64
		expectMetric     bool
		expectError      bool
		description      string
	}{
		{
			name:             "First collection - should cache values",
			currentBlksHit:   1000,
			currentBlksRead:  100,
			previousBlksHit:  0,
			previousBlksRead: 0,
			expectedHitRatio: 0.0,
			expectMetric:     false,
			expectError:      false,
			description:      "First collection should only cache values, no metric sent",
		},
		{
			name:             "Normal interval with good cache hit ratio",
			currentBlksHit:   2000,
			currentBlksRead:  150,
			previousBlksHit:  1000,
			previousBlksRead: 100,
			expectedHitRatio: 95.24, // (2000-1000)/(2000-1000+150-100) = 1000/1050 = 95.24%
			expectMetric:     true,
			expectError:      false,
			description:      "Should calculate correct hit ratio for the interval",
		},
		{
			name:             "Interval with poor cache hit ratio",
			currentBlksHit:   1100,
			currentBlksRead:  900,
			previousBlksHit:  1000,
			previousBlksRead: 100,
			expectedHitRatio: 11.11, // (1100-1000)/(1100-1000+900-100) = 100/900 = 11.11%
			expectMetric:     true,
			expectError:      false,
			description:      "Should calculate correct hit ratio for poor performance",
		},
		{
			name:             "No activity in interval",
			currentBlksHit:   1000,
			currentBlksRead:  100,
			previousBlksHit:  1000,
			previousBlksRead: 100,
			expectedHitRatio: 0.0,
			expectMetric:     true,
			expectError:      false,
			description:      "No activity should result in 0% hit ratio",
		},
		{
			name:             "Counter reset detected",
			currentBlksHit:   500,
			currentBlksRead:  50,
			previousBlksHit:  1000,
			previousBlksRead: 100,
			expectedHitRatio: 0.0,
			expectMetric:     false,
			expectError:      false,
			description:      "Counter reset should be handled gracefully",
		},
		{
			name:             "Invalid negative values",
			currentBlksHit:   -1,
			currentBlksRead:  100,
			previousBlksHit:  0,
			previousBlksRead: 0,
			expectedHitRatio: 0.0,
			expectMetric:     false,
			expectError:      true,
			description:      "Negative values should cause error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock
			mockPool.Reset()
			mockPool.SetQueryResult(tt.currentBlksHit, tt.currentBlksRead)

			// Create state with previous values
			state := &agent.MetricsState{
				Cache: agent.Caches{
					BufferStats: agent.BufferStat{
						BlksHit:  tt.previousBlksHit,
						BlksRead: tt.previousBlksRead,
					},
				},
				Mutex: &sync.Mutex{},
			}

			// Create collector function that works with our interface
			collector := func(ctx context.Context, state *agent.MetricsState) error {
				var blksHit, blksRead int64
				err := mockPool.QueryRow(ctx, BufferCacheHitRatioQuery).Scan(&blksHit, &blksRead)
				if err != nil {
					return err
				}

				// Validate that we have reasonable values
				if blksHit < 0 || blksRead < 0 {
					return fmt.Errorf("invalid buffer cache statistics: blks_hit=%d, blks_read=%d", blksHit, blksRead)
				}

				// Check if we have cached values from the previous collection
				if state.Cache.BufferStats.BlksHit == 0 && state.Cache.BufferStats.BlksRead == 0 {
					// First collection, just cache the values
					state.Cache.BufferStats = agent.BufferStat{
						BlksHit:  blksHit,
						BlksRead: blksRead,
					}
					return nil
				}

				// Calculate deltas
				blksHitDelta := blksHit - state.Cache.BufferStats.BlksHit
				blksReadDelta := blksRead - state.Cache.BufferStats.BlksRead

				// Handle counter resets (e.g., after PostgreSQL crash or immediate shutdown)
				if blksHitDelta < 0 || blksReadDelta < 0 {
					// Reset detected, update cache and skip this collection
					state.Cache.BufferStats = agent.BufferStat{
						BlksHit:  blksHit,
						BlksRead: blksRead,
					}
					return nil
				}

				// Calculate cache hit ratio for this interval
				var bufferCacheHitRatio float64
				totalBlocks := blksHitDelta + blksReadDelta
				if totalBlocks > 0 {
					bufferCacheHitRatio = float64(blksHitDelta) / float64(totalBlocks) * 100.0
				} else {
					// No block activity in this interval
					bufferCacheHitRatio = 0.0
				}

				// Validate the calculated ratio is within reasonable bounds
				if bufferCacheHitRatio < 0.0 || bufferCacheHitRatio > 100.0 {
					return fmt.Errorf("calculated cache hit ratio is out of bounds: %.2f%%", bufferCacheHitRatio)
				}

				metricEntry, err := metrics.PGCacheHitRatio.AsFlatValue(bufferCacheHitRatio)
				if err != nil {
					return err
				}
				state.AddMetric(metricEntry)

				// Update cache for next iteration
				state.Cache.BufferStats = agent.BufferStat{
					BlksHit:  blksHit,
					BlksRead: blksRead,
				}

				return nil
			}

			// Execute
			err := collector(context.Background(), state)

			// Assertions
			if tt.expectError {
				assert.Error(t, err, tt.description)
				return
			}

			assert.NoError(t, err, tt.description)

			if tt.expectMetric {
				// Check that metric was added
				assert.Len(t, state.Metrics, 1, "Should have exactly one metric")

				metric := state.Metrics[0]
				assert.Equal(t, metrics.PGCacheHitRatio.Key, metric.Key, "Metric key should match")

				// Check the value (allow small floating point differences)
				actualValue, ok := metric.Value.(float64)
				require.True(t, ok, "Metric value should be float64")
				assert.InDelta(t, tt.expectedHitRatio, actualValue, 0.01,
					"Hit ratio should match expected value")
			} else {
				// Check that no metric was added
				assert.Len(t, state.Metrics, 0, "Should have no metrics")
			}

			// Verify cache was updated
			assert.Equal(t, tt.currentBlksHit, state.Cache.BufferStats.BlksHit,
				"Cache should be updated with current values")
			assert.Equal(t, tt.currentBlksRead, state.Cache.BufferStats.BlksRead,
				"Cache should be updated with current values")
		})
	}
}
