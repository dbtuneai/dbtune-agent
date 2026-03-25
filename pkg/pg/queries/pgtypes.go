package queries

import (
	"encoding/json"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQL defined types — provide self-documenting struct fields and
// type safety. pgx's TryFindUnderlyingTypeScanPlan unwraps these to their
// underlying primitive kinds for binary-format scanning.
//
// Types whose PG binary codec doesn't accept the underlying Go primitive
// (e.g. inet → InetCodec rejects *string) implement the codec's scanner
// interface so PlanScan matches directly.
//
// Use pointers (*Oid, *Name, etc.) for nullable columns.
//
// NOTE: pg numeric is arbitrary precision. There is no single Go type that
// fits all cases — use the appropriate Go primitive per column (e.g. Bigint
// for large counters like wal_bytes, DoublePrecision for fractional values).

type Oid uint32              // pg: oid — object identifier
type Name string             // pg: name — 63-byte SQL identifier
type Text string             // pg: text — variable-length string
type Bigint int64            // pg: bigint / int8
type Integer int64           // pg: integer / int4
type Smallint int64          // pg: smallint / int2
type Real float64            // pg: real / float4
type DoublePrecision float64 // pg: double precision / float8
type Boolean bool            // pg: boolean
type PgLsn string            // pg: pg_lsn — log sequence number

// Inet wraps string for pg inet columns.
// Implements pgtype.NetipPrefixScanner so InetCodec's binary scan matches.
type Inet string

func (i *Inet) ScanNetipPrefix(v netip.Prefix) error {
	if !v.IsValid() {
		*i = ""
		return nil
	}
	// Match PG text format: omit mask for host addresses (/32 or /128).
	addr := v.Addr()
	if v.Bits() == addr.BitLen() {
		*i = Inet(addr.String())
	} else {
		*i = Inet(v.String())
	}
	return nil
}

// Xid wraps string for pg xid (transaction ID) columns.
// Implements pgtype.TextScanner so Uint32Codec's binary scan matches.
type Xid string

func (x *Xid) ScanText(v pgtype.Text) error {
	if !v.Valid {
		*x = ""
		return nil
	}
	*x = Xid(v.String)
	return nil
}

// Interval stores a pg interval as total microseconds.
// Implements pgtype.IntervalScanner so IntervalCodec's binary scan matches.
// PostgreSQL intervals have independent months/days/microseconds components;
// this type normalises them to a single microsecond count using the same
// conversion factors as PostgreSQL's EXTRACT(EPOCH): 1 month = 30 days,
// 1 day = 24 hours.
type Interval int64

const (
	microsecondsPerDay   = 24 * 60 * 60 * 1_000_000 // 86 400 000 000
	microsecondsPerMonth = 30 * microsecondsPerDay  // 2 592 000 000 000
)

func (iv *Interval) ScanInterval(v pgtype.Interval) error {
	if !v.Valid {
		*iv = 0
		return nil
	}
	*iv = Interval(int64(v.Months)*microsecondsPerMonth +
		int64(v.Days)*microsecondsPerDay +
		v.Microseconds)
	return nil
}

// TimestampTZ wraps time.Time for pg timestamptz columns.
// Implements pgtype.TimestamptzScanner so TimestamptzCodec's binary scan matches.
type TimestampTZ time.Time

func (t *TimestampTZ) ScanTimestamptz(v pgtype.Timestamptz) error {
	if !v.Valid {
		*t = TimestampTZ{}
		return nil
	}
	*t = TimestampTZ(v.Time)
	return nil
}

func (t TimestampTZ) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t))
}

// Anyarray and Float4Array remain json.RawMessage — they require
// special conversion logic in pg_stats.go.
type Anyarray = json.RawMessage
type Float4Array = json.RawMessage
