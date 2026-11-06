package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePGVersion(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		want       Version
		wantString string
		wantErr    bool
	}{
		{"release", "PostgreSQL 16.4 on x86_64-pc-linux-gnu, compiled by gcc", Version{Major: 16, Minor: 4, HasMinor: true}, "16.4", false},
		{"rds", "PostgreSQL 12.22 on aarch64-unknown-linux-gnu, compiled by gcc (GCC) 7.3.1, 64-bit", Version{Major: 12, Minor: 22, HasMinor: true}, "12.22", false},
		{"zero minor", "PostgreSQL 17.0 on x86_64-pc-linux-gnu", Version{Major: 17, Minor: 0, HasMinor: true}, "17.0", false},
		{"beta", "PostgreSQL 18beta1 on x86_64-pc-linux-gnu", Version{Major: 18}, "18", false},
		{"rc", "PostgreSQL 17rc1 on x86_64-pc-linux-gnu", Version{Major: 17}, "17", false},
		{"devel", "PostgreSQL 19devel on x86_64-pc-linux-gnu", Version{Major: 19}, "19", false},
		{"unrecognized", "not postgres", Version{}, "", true},
		{"empty", "", Version{}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePGVersion(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantString, got.String())
		})
	}
}
