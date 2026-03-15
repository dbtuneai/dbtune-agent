package queries

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashDDL(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		checkFn func(t *testing.T, hash string)
	}{
		{
			name: "deterministic: same input same hash",
			ddl:  "CREATE TABLE users (id int PRIMARY KEY);",
			checkFn: func(t *testing.T, hash string) {
				again := HashDDL("CREATE TABLE users (id int PRIMARY KEY);")
				assert.Equal(t, hash, again)
			},
		},
		{
			name: "different input different hash",
			ddl:  "CREATE TABLE users (id int);",
			checkFn: func(t *testing.T, hash string) {
				other := HashDDL("CREATE TABLE orders (id int);")
				assert.NotEqual(t, hash, other)
			},
		},
		{
			name: "empty string produces valid hash",
			ddl:  "",
			checkFn: func(t *testing.T, hash string) {
				assert.NotEmpty(t, hash)
			},
		},
		{
			name: "hash is 64-char hex (SHA-256)",
			ddl:  "CREATE INDEX idx ON t(c);",
			checkFn: func(t *testing.T, hash string) {
				require.Len(t, hash, 64)
				for _, ch := range hash {
					assert.True(t, (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'),
						"unexpected char %c in hash", ch)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := HashDDL(tt.ddl)
			tt.checkFn(t, hash)
		})
	}
}
