package pg

import (
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/metrics"
)

// EmitMetric formats and adds a single metric to the state.
func EmitMetric(state *agent.MetricsState, metric metrics.MetricDef, value any) error {
	entry, err := metric.AsFlatValue(value)
	if err == nil {
		state.AddMetric(entry)
	} else {
		return err
	}
	return nil
}

type Number interface {
	~int64 | ~float64
}

type MetricMapping[T Number] struct {
	Current  *T
	Previous *T
	Metric   metrics.MetricDef
}

// EmitCumulativeMetricsMap calculates deltas and emits cumulative metrics.
// If either current or previous is nil, the metric is skipped.
// Negative deltas (counter resets) are also skipped.
func EmitCumulativeMetricsMap[T Number](state *agent.MetricsState, mappings []MetricMapping[T]) error {
	for _, m := range mappings {
		if m.Previous != nil && m.Current != nil {
			delta := *m.Current - *m.Previous
			if *m.Previous >= 0 && *m.Current >= 0 && delta >= 0 {
				err := EmitMetric(state, m.Metric, delta)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
