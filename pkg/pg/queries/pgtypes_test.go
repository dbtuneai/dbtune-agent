package queries

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestInterval_ScanInterval_Null(t *testing.T) {
	var iv Interval
	err := iv.ScanInterval(pgtype.Interval{Valid: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if iv != 0 {
		t.Fatalf("null interval should be 0, got %d", iv)
	}
}

func TestInterval_ScanInterval_PureMicroseconds(t *testing.T) {
	var iv Interval
	err := iv.ScanInterval(pgtype.Interval{
		Microseconds: 1_500_000, // 1.5 seconds
		Valid:        true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if iv != 1_500_000 {
		t.Fatalf("expected 1500000, got %d", iv)
	}
}

func TestInterval_ScanInterval_DaysAndMicroseconds(t *testing.T) {
	var iv Interval
	err := iv.ScanInterval(pgtype.Interval{
		Days:         2,
		Microseconds: 3_600_000_000, // 1 hour
		Valid:        true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 2 days = 2 * 86_400_000_000 = 172_800_000_000
	// 1 hour = 3_600_000_000
	// total  = 176_400_000_000
	expected := Interval(176_400_000_000)
	if iv != expected {
		t.Fatalf("expected %d, got %d", expected, iv)
	}
}

func TestInterval_ScanInterval_MonthsDaysMicroseconds(t *testing.T) {
	var iv Interval
	err := iv.ScanInterval(pgtype.Interval{
		Months:       1,
		Days:         5,
		Microseconds: 1_000_000, // 1 second
		Valid:        true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1 month = 30 * 86_400_000_000 = 2_592_000_000_000
	// 5 days  = 5 * 86_400_000_000  =   432_000_000_000
	// 1 sec   =                              1_000_000
	// total   =                      3_024_001_000_000
	expected := Interval(2_592_000_000_000 + 432_000_000_000 + 1_000_000)
	if iv != expected {
		t.Fatalf("expected %d, got %d", expected, iv)
	}
}

func TestInterval_ScanInterval_Negative(t *testing.T) {
	var iv Interval
	err := iv.ScanInterval(pgtype.Interval{
		Microseconds: -500_000,
		Valid:        true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if iv != -500_000 {
		t.Fatalf("expected -500000, got %d", iv)
	}
}

func TestInterval_ScanInterval_ZeroDuration(t *testing.T) {
	var iv Interval
	err := iv.ScanInterval(pgtype.Interval{Valid: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if iv != 0 {
		t.Fatalf("zero interval should be 0, got %d", iv)
	}
}
