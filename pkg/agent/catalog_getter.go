package agent

import (
	"github.com/dbtuneai/agent/pkg/pg/queries"
)

// CatalogGetter is embedded in adapters to provide CatalogCollectors().
// Adapters populate the list at construction time using
// pg.StandardCatalogCollectors or by building the list directly.
type CatalogGetter struct {
	collectors []queries.CatalogCollector
}

// SetCatalogCollectors stores the pre-built list of catalog collectors.
func (g *CatalogGetter) SetCatalogCollectors(cc []queries.CatalogCollector) {
	g.collectors = cc
}

// CatalogCollectors returns the catalog collectors for this adapter.
func (g *CatalogGetter) CatalogCollectors() []queries.CatalogCollector {
	return g.collectors
}
