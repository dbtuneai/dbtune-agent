package catalog

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strconv"
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

type Oid uint32               // pg: oid — object identifier
type Name string              // pg: name — 63-byte SQL identifier
type Text string              // pg: text — variable-length string
type Bigint int64             // pg: bigint / int8
type Integer int64            // pg: integer / int4
type Smallint int64           // pg: smallint / int2
type Real float64             // pg: real / float4
type DoublePrecision float64  // pg: double precision / float8
type Boolean bool             // pg: boolean
type PgLsn string             // pg: pg_lsn — log sequence number
type Numeric int64            // pg: numeric — arbitrary precision (lossy: mapped to int64)

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

// Interval wraps string for pg interval columns.
// Implements pgtype.IntervalScanner so IntervalCodec's binary scan matches.
type Interval string

func (iv *Interval) ScanInterval(v pgtype.Interval) error {
	if !v.Valid {
		*iv = ""
		return nil
	}
	// Reproduce PG interval text format (matches IntervalCodec text encoder).
	var buf []byte
	if v.Months != 0 {
		buf = append(buf, strconv.FormatInt(int64(v.Months), 10)...)
		buf = append(buf, " mon "...)
	}
	if v.Days != 0 {
		buf = append(buf, strconv.FormatInt(int64(v.Days), 10)...)
		buf = append(buf, " day "...)
	}
	absMicroseconds := v.Microseconds
	if absMicroseconds < 0 {
		absMicroseconds = -absMicroseconds
		buf = append(buf, '-')
	}
	hours := absMicroseconds / 3600000000
	minutes := (absMicroseconds % 3600000000) / 60000000
	seconds := (absMicroseconds % 60000000) / 1000000
	buf = append(buf, fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)...)
	microseconds := absMicroseconds % 1000000
	if microseconds != 0 {
		buf = append(buf, fmt.Sprintf(".%06d", microseconds)...)
	}
	*iv = Interval(string(buf))
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
