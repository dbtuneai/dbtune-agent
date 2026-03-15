package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtuneai/agent/pkg/pg"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	return l
}

// ---------------------------------------------------------------------------
// CatalogGetter.prepareCtx integration
// ---------------------------------------------------------------------------

func TestPrepareCtx_NilHealthGate_NilPrepareContext(t *testing.T) {
	g := &CatalogGetter{}
	ctx := context.Background()

	got, err := g.prepareCtx(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != ctx {
		t.Fatal("should return original context when both HealthGate and PrepareContext are nil")
	}
}

func TestPrepareCtx_HealthGateOpen_PassesThrough(t *testing.T) {
	hg := pg.NewHealthGate(nil, testLogger())

	var prepareCalled bool
	g := &CatalogGetter{
		HealthGate: hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			prepareCalled = true
			return ctx, nil
		},
	}

	_, err := g.prepareCtx(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !prepareCalled {
		t.Fatal("PrepareContext should be called when gate is open")
	}
}

func TestPrepareCtx_HealthGateDown_ShortCircuits(t *testing.T) {
	hg := pg.NewHealthGate(nil, testLogger())
	hg.Start(context.Background())
	defer hg.Stop()
	// Trip the gate via public API
	hg.ReportError(&pgconn.ConnectError{})

	var prepareCalled bool
	g := &CatalogGetter{
		HealthGate: hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			prepareCalled = true
			return ctx, nil
		},
	}

	_, err := g.prepareCtx(context.Background())
	if !errors.Is(err, pg.ErrDatabaseDown) {
		t.Fatalf("expected ErrDatabaseDown, got %v", err)
	}
	if prepareCalled {
		t.Fatal("PrepareContext should NOT be called when gate is down")
	}
}

func TestPrepareCtx_PrepareContextError_PropagatedWhenGateOpen(t *testing.T) {
	hg := pg.NewHealthGate(nil, testLogger())
	expectedErr := errors.New("failover in progress")

	g := &CatalogGetter{
		HealthGate: hg,
		PrepareContext: func(ctx context.Context) (context.Context, error) {
			return nil, expectedErr
		},
	}

	_, err := g.prepareCtx(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected PrepareContext error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CatalogGetter.StartHealthGate
// ---------------------------------------------------------------------------

func TestStartHealthGate_NilHealthGate(t *testing.T) {
	g := &CatalogGetter{}
	g.StartHealthGate(context.Background()) // Should not panic.
}

func TestStartHealthGate_WithHealthGate(t *testing.T) {
	hg := pg.NewHealthGate(nil, testLogger())
	g := &CatalogGetter{HealthGate: hg}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.StartHealthGate(ctx)
	// If Start() didn't panic, the delegation worked.
}

// ---------------------------------------------------------------------------
// CatalogCollectors
// ---------------------------------------------------------------------------

func TestCatalogCollectors_NilHealthGate_NoWrapping(t *testing.T) {
	g := &CatalogGetter{
		PGMajorVersion: 16,
		HealthGate:     nil,
	}

	// This should not panic — collectors are returned without wrapping.
	collectors := g.CatalogCollectors()
	if len(collectors) == 0 {
		t.Fatal("expected collectors even with nil HealthGate")
	}
}

func TestCatalogCollectors_WithHealthGate(t *testing.T) {
	hg := pg.NewHealthGate(nil, testLogger())
	hg.Start(context.Background())
	defer hg.Stop()

	g := &CatalogGetter{
		PGMajorVersion: 16,
		HealthGate:     hg,
	}

	collectors := g.CatalogCollectors()
	if len(collectors) == 0 {
		t.Fatal("expected collectors")
	}
}

// ---------------------------------------------------------------------------
// CatalogCollectors wiring shape tests
// ---------------------------------------------------------------------------

func TestCatalogCollectors_Shape(t *testing.T) {
	g := &CatalogGetter{
		PGMajorVersion: 16,
	}
	collectors := g.CatalogCollectors()

	t.Run("exact count", func(t *testing.T) {
		if len(collectors) != 37 {
			t.Fatalf("expected 37 collectors, got %d", len(collectors))
		}
	})

	// Build a lookup map
	byName := make(map[string]CatalogCollector, len(collectors))
	for _, c := range collectors {
		byName[c.Name] = c
	}

	t.Run("key collectors present", func(t *testing.T) {
		required := []string{
			"ddl",
			"pg_stats",
			"pg_class",
			"pg_stat_statements",
			"database_transactions",
			"pg_index",
		}
		for _, name := range required {
			if _, ok := byName[name]; !ok {
				t.Errorf("missing expected collector %q", name)
			}
		}
	})

	t.Run("ddl has SkipUnchanged", func(t *testing.T) {
		if c, ok := byName["ddl"]; ok && !c.SkipUnchanged {
			t.Error("ddl collector should have SkipUnchanged=true")
		}
	})

	t.Run("pg_index has SkipUnchanged", func(t *testing.T) {
		if c, ok := byName["pg_index"]; ok && !c.SkipUnchanged {
			t.Error("pg_index collector should have SkipUnchanged=true")
		}
	})

	t.Run("all collectors have non-empty Name and positive Interval", func(t *testing.T) {
		for i, c := range collectors {
			if c.Name == "" {
				t.Errorf("collector[%d] has empty Name", i)
			}
			if c.Interval <= 0 {
				t.Errorf("collector[%d] (%s) has non-positive Interval: %v", i, c.Name, c.Interval)
			}
		}
	})

	t.Run("no duplicate names", func(t *testing.T) {
		seen := make(map[string]bool, len(collectors))
		for _, c := range collectors {
			if seen[c.Name] {
				t.Errorf("duplicate collector name: %q", c.Name)
			}
			seen[c.Name] = true
		}
	})
}
