package agent

// PgLocksRow represents a filtered row from pg_locks (blocked + blockers only).
type PgLocksRow struct {
	LockType           *string `json:"locktype" db:"locktype"`
	Database           *int64  `json:"database" db:"database"`
	Relation           *int64  `json:"relation" db:"relation"`
	Page               *int64  `json:"page" db:"page"`
	Tuple              *int64  `json:"tuple" db:"tuple"`
	VirtualXID         *string `json:"virtualxid" db:"virtualxid"`
	TransactionID      *string `json:"transactionid" db:"transactionid"`
	ClassID            *int64  `json:"classid" db:"classid"`
	ObjID              *int64  `json:"objid" db:"objid"`
	ObjSubID           *int64  `json:"objsubid" db:"objsubid"`
	VirtualTransaction *string `json:"virtualtransaction" db:"virtualtransaction"`
	PID                *int64  `json:"pid" db:"pid"`
	Mode               *string `json:"mode" db:"mode"`
	Granted            *bool   `json:"granted" db:"granted"`
	FastPath           *bool   `json:"fastpath" db:"fastpath"`
	WaitStart          *string `json:"waitstart" db:"waitstart"`
}

type PgLocksPayload struct {
	Rows []PgLocksRow `json:"rows"`
}
