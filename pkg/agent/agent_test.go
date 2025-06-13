package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/metrics"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// mockCollector creates a collector that simulates different behaviors
func mockCollector(delay time.Duration, shouldError bool, metrics []metrics.FlatValue) MetricCollector {
	return MetricCollector{
		Key: "test_collector",
		Collector: func(ctx context.Context, state *MetricsState) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				if shouldError {
					return errors.New("collector error")
				}
				for _, metric := range metrics {
					state.AddMetric(metric)
				}
				return nil
			}
		},
	}
}

func TestGetMetrics(t *testing.T) {
	t.Run("happy path - all collectors succeed", func(t *testing.T) {
		agent := &CommonAgent{
			logger: logrus.New(),
			MetricsState: MetricsState{
				Collectors: []MetricCollector{
					mockCollector(100*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric1",
						Value: 1,
						Type:  "int",
					}}),
					mockCollector(200*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric2",
						Value: 2,
						Type:  "int",
					}}),
				},
				Mutex: &sync.Mutex{},
			},
			CollectionTimeout: 1 * time.Second,
			IndividualTimeout: 500 * time.Millisecond,
		}

		flat_metrics, err := agent.GetMetrics()
		assert.NoError(t, err)
		assert.Len(t, flat_metrics, 2)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("partial failure - one collector errors", func(t *testing.T) {
		agent := &CommonAgent{
			logger: logrus.New(),
			MetricsState: MetricsState{
				Collectors: []MetricCollector{
					mockCollector(100*time.Millisecond, true, nil), // This one will error
					mockCollector(200*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric2",
						Value: 2,
						Type:  "int",
					}}),
				},
				Mutex: &sync.Mutex{},
			},
			CollectionTimeout: 1 * time.Second,
			IndividualTimeout: 500 * time.Millisecond,
		}

		flat_metrics, err := agent.GetMetrics()
		assert.NoError(t, err) // The function should not return error even if collectors fail
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric2", Value: 2, Type: "int"})
	})

	t.Run("context timeout - slow collectors are cancelled", func(t *testing.T) {
		agent := &CommonAgent{
			logger: logrus.New(),
			MetricsState: MetricsState{
				Collectors: []MetricCollector{
					mockCollector(100*time.Millisecond, false, []metrics.FlatValue{{
						Key:   "metric1",
						Value: 1,
						Type:  "int",
					}}),
					mockCollector(600*time.Millisecond, false, []metrics.FlatValue{{ // This one will timeout as it exceeds IndividualTimeout
						Key:   "metric2",
						Value: 2,
						Type:  "int",
					}}),
				},
				Mutex: &sync.Mutex{},
			},
			CollectionTimeout: 1 * time.Second,
			IndividualTimeout: 500 * time.Millisecond,
		}

		flat_metrics, err := agent.GetMetrics()
		assert.NoError(t, err)
		assert.Len(t, flat_metrics, 1)
		assert.Contains(t, flat_metrics, metrics.FlatValue{Key: "metric1", Value: 1, Type: "int"})
	})
}
