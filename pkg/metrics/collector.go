package metrics

import (
	"context"
	"sync"
)

type IOCounterStat struct {
	ReadCount  uint64
	WriteCount uint64
}

// Caches is a struct that holds the caches for the agent
// that is updated between each metric collection beat.
// Currently, this is fixed for all adapters.
// TODO: Make this dynamic for each adapter, this could use
// GJSON and SJSON to update the cache as a string, but the locking then would be a problem in reading and writing without custom methods on state.
type Caches struct {
	IOCountersStat IOCounterStat

	// Hardware specific cache for guardrails
	// {
	// 	"total_memory": 1024,
	// 	"disk_size": 1024,
	// }
	HardwareCache map[string]interface{}
}

type Collector struct {
	Key       string
	Collector func(ctx context.Context, state *State) error
}

type State struct {
	Collectors []Collector
	// Caching layer, for metrics that are derived based on multiple heartbeats
	Cache Caches
	// Every round of metric collections this array will be filled with the metrics
	// that are collected, and then emptied
	Metrics []FlatValue
	Mutex   *sync.Mutex
}

// AddMetric appends a metric in a thread-safe way
func (state *State) AddMetric(metric FlatValue) {
	state.Mutex.Lock()
	defer state.Mutex.Unlock()
	state.Metrics = append(state.Metrics, metric)
}
