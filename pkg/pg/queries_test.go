package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPGMajorVersion(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    int
		wantErr bool
	}{
		{"minor suffix", "16.4", 16, false},
		{"major only", "17", 17, false},
		{"three parts", "13.14.2", 13, false},
		{"empty", "", 0, true},
		{"non-numeric", "abc", 0, true},
		{"leading dot", ".16", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PGMajorVersion(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
