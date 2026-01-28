package utils

import (
	"testing"
)

func TestFilterTopTables(t *testing.T) {
	t.Run("returns original map when under limit", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"table_a_1": {NLiveTup: 100, NDeadTup: 10},
			"table_b_2": {NLiveTup: 200, NDeadTup: 20},
		}

		result := FilterTopTables(tables, 5)

		if len(result) != 2 {
			t.Errorf("expected 2 tables, got %d", len(result))
		}
		if _, exists := result["table_a_1"]; !exists {
			t.Error("expected table_a_1 to exist")
		}
		if _, exists := result["table_b_2"]; !exists {
			t.Error("expected table_b_2 to exist")
		}
	})

	t.Run("returns original map when at limit", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"table_a_1": {NLiveTup: 100, NDeadTup: 10},
			"table_b_2": {NLiveTup: 200, NDeadTup: 20},
			"table_c_3": {NLiveTup: 300, NDeadTup: 30},
		}

		result := FilterTopTables(tables, 3)

		if len(result) != 3 {
			t.Errorf("expected 3 tables, got %d", len(result))
		}
	})

	t.Run("filters to top N tables by total tuples", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"small_1":  {NLiveTup: 10, NDeadTup: 5},
			"medium_2": {NLiveTup: 100, NDeadTup: 50},
			"large_3":  {NLiveTup: 1000, NDeadTup: 500},
			"tiny_4":   {NLiveTup: 1, NDeadTup: 0},
		}

		result := FilterTopTables(tables, 2)

		if len(result) != 2 {
			t.Errorf("expected 2 tables, got %d", len(result))
		}
		if _, exists := result["large_3"]; !exists {
			t.Error("expected large_3 to be kept (highest tuple count)")
		}
		if _, exists := result["medium_2"]; !exists {
			t.Error("expected medium_2 to be kept (second highest tuple count)")
		}
		if _, exists := result["small_1"]; exists {
			t.Error("expected small_1 to be filtered out")
		}
		if _, exists := result["tiny_4"]; exists {
			t.Error("expected tiny_4 to be filtered out")
		}
	})

	t.Run("uses key as tiebreaker when tuple counts are equal", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"zebra_1": {NLiveTup: 100, NDeadTup: 0},
			"alpha_2": {NLiveTup: 100, NDeadTup: 0},
			"beta_3":  {NLiveTup: 100, NDeadTup: 0},
			"gamma_4": {NLiveTup: 100, NDeadTup: 0},
		}

		result := FilterTopTables(tables, 2)

		if len(result) != 2 {
			t.Errorf("expected 2 tables, got %d", len(result))
		}
		// When tuple counts are equal, should keep alphabetically first keys
		if _, exists := result["alpha_2"]; !exists {
			t.Error("expected alpha_2 to be kept (alphabetically first)")
		}
		if _, exists := result["beta_3"]; !exists {
			t.Error("expected beta_3 to be kept (alphabetically second)")
		}
	})

	t.Run("considers both live and dead tuples for sorting", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"high_live_1": {NLiveTup: 1000, NDeadTup: 0},
			"high_dead_2": {NLiveTup: 0, NDeadTup: 1000},
			"balanced_3":  {NLiveTup: 500, NDeadTup: 500},
			"small_4":     {NLiveTup: 50, NDeadTup: 50},
		}

		result := FilterTopTables(tables, 3)

		if len(result) != 3 {
			t.Errorf("expected 3 tables, got %d", len(result))
		}
		// All three have total of 1000, small_4 has 100
		if _, exists := result["small_4"]; exists {
			t.Error("expected small_4 to be filtered out (lowest total)")
		}
	})

	t.Run("handles empty map", func(t *testing.T) {
		tables := map[string]PGUserTables{}

		result := FilterTopTables(tables, 5)

		if len(result) != 0 {
			t.Errorf("expected 0 tables, got %d", len(result))
		}
	})

	t.Run("handles limit of 1", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"small_1": {NLiveTup: 10, NDeadTup: 5},
			"large_2": {NLiveTup: 1000, NDeadTup: 500},
		}

		result := FilterTopTables(tables, 1)

		if len(result) != 1 {
			t.Errorf("expected 1 table, got %d", len(result))
		}
		if _, exists := result["large_2"]; !exists {
			t.Error("expected large_2 to be the only table kept")
		}
	})

	t.Run("preserves table stats data when filtering", func(t *testing.T) {
		tables := map[string]PGUserTables{
			"small_1": {NLiveTup: 10, NDeadTup: 5, SeqScan: 100, IdxScan: 200},
			"large_2": {NLiveTup: 1000, NDeadTup: 500, SeqScan: 999, IdxScan: 888},
		}

		result := FilterTopTables(tables, 1)

		if result["large_2"].SeqScan != 999 {
			t.Errorf("expected SeqScan to be 999, got %d", result["large_2"].SeqScan)
		}
		if result["large_2"].IdxScan != 888 {
			t.Errorf("expected IdxScan to be 888, got %d", result["large_2"].IdxScan)
		}
	})
}
