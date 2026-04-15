package agent

import (
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/pg/queries"
	"github.com/stretchr/testify/assert"
)

func TestCatalogGetter_RoundTrip(t *testing.T) {
	g := &CatalogGetter{}
	assert.Empty(t, g.CatalogCollectors(), "empty getter should return empty slice")

	want := []queries.CatalogCollector{
		{Name: "a", Interval: time.Second},
		{Name: "b", Interval: 2 * time.Second},
	}
	g.SetCatalogCollectors(want)

	got := g.CatalogCollectors()
	assert.Equal(t, want, got)
}

func TestCatalogGetter_NilReceiver(t *testing.T) {
	var g CatalogGetter
	assert.Empty(t, g.CatalogCollectors())
}
