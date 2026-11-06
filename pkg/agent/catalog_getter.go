package agent

import (
	"github.com/dbtuneai/agent/pkg/catalog"
)

// CatalogGetter is embedded in adapters to provide CatalogCollectors().
// Adapters populate the list at construction time using
// catalog.StandardCollectors or by building the list directly.
type CatalogGetter struct {
	collectors []catalog.Collector
}

// SetCatalogCollectors stores the pre-built list of catalog collectors.
func (g *CatalogGetter) SetCatalogCollectors(cc []catalog.Collector) {
	g.collectors = cc
}

// CatalogCollectors returns the catalog collectors for this adapter.
func (g *CatalogGetter) CatalogCollectors() []catalog.Collector {
	return g.collectors
}
