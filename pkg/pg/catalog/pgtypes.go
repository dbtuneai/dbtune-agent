package catalog

import "encoding/json"

// PostgreSQL type aliases — provide self-documenting struct fields without
// needing // pg: comments. Type aliases (=) are transparent to pgx's scanner,
// so *Oid scans identically to *int64 in both binary and text formats.
//
// Use pointers (*Oid, *Name, etc.) for nullable columns.

type Oid = uint32              // pg: oid — object identifier
type Name = string             // pg: name — 63-byte SQL identifier
type Text = string             // pg: text — variable-length string
type Bigint = int64            // pg: bigint / int8
type Integer = int64           // pg: integer / int4
type Smallint = int64          // pg: smallint / int2
type Real = float64            // pg: real / float4
type DoublePrecision = float64 // pg: double precision / float8
type Boolean = bool            // pg: boolean
type TimestampTZ = string      // pg: timestamp with time zone
type Xid = string              // pg: xid — transaction ID
type Inet = string             // pg: inet — IPv4/IPv6 host address
type PgLsn = string            // pg: pg_lsn — log sequence number
type Interval = string         // pg: interval — time span
type Numeric = int64           // pg: numeric — arbitrary precision (lossy: mapped to int64)

// Anyarray and Float4Array remain json.RawMessage — they require
// special conversion logic in pg_stats.go.
type Anyarray = json.RawMessage
type Float4Array = json.RawMessage
