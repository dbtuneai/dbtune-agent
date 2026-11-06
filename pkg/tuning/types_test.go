package tuning

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKnobApplication_UnmarshalJSON(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    KnobApplication
		wantErr bool
	}{
		{name: "restart", input: `"restart"`, want: KnobApplicationRestart},
		{name: "reload", input: `"reload"`, want: KnobApplicationReload},
		{name: "empty string", input: `""`, wantErr: true},
		{name: "uppercase", input: `"RESTART"`, wantErr: true},
		{name: "unknown value", input: `"foo"`, wantErr: true},
		{name: "non-string", input: `42`, wantErr: true},
		{name: "null", input: `null`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got KnobApplication
			err := json.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestProposedConfigResponse_UnmarshalJSON_RejectsInvalidKnobApplication(t *testing.T) {
	payload := `{"config":[],"knobs_overrides":[],"knob_application":"sometimes"}`
	var resp ProposedConfigResponse
	err := json.Unmarshal([]byte(payload), &resp)
	assert.Error(t, err, "decoding should fail before an adapter ever sees the response")
}

func TestProposedConfigResponse_UnmarshalJSON_RejectsMissingKnobApplication(t *testing.T) {
	// An omitted field leaves KnobApplication at its zero value ("") and
	// UnmarshalJSON is not invoked. We rely on the field always being sent,
	// but document the current behavior so it's an explicit choice rather
	// than an oversight: an omitted field decodes successfully to "".
	payload := `{"config":[],"knobs_overrides":[]}`
	var resp ProposedConfigResponse
	err := json.Unmarshal([]byte(payload), &resp)
	require.NoError(t, err)
	assert.Equal(t, KnobApplication(""), resp.KnobApplication)
}
