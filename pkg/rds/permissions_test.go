package rds

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeParameterGroupReader records what the check sent and returns canned errors.
type fakeParameterGroupReader struct {
	err error
	in  *rds.DescribeDBParametersInput
}

func (f *fakeParameterGroupReader) DescribeDBParameters(
	_ context.Context, in *rds.DescribeDBParametersInput, _ ...func(*rds.Options),
) (*rds.DescribeDBParametersOutput, error) {
	f.in = in
	if f.err != nil {
		return nil, f.err
	}
	return &rds.DescribeDBParametersOutput{}, nil
}

func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: code}
}

func TestCheckDescribeDBParametersAccess_ReadsTheAttachedGroup(t *testing.T) {
	client := &fakeParameterGroupReader{}
	err := checkDescribeDBParametersAccess(context.Background(), client, "my-pg")
	require.NoError(t, err)

	require.NotNil(t, client.in)
	assert.Equal(t, "my-pg", aws.ToString(client.in.DBParameterGroupName))
	// Unfiltered on purpose: the check must not depend on filter support.
	assert.Empty(t, client.in.Filters)
}

func TestCheckDescribeDBParametersAccess_NamesThePermissionWhenDenied(t *testing.T) {
	// Codes from the RDS Common Error Types page, plus the AccessDenied alias.
	for _, code := range []string{"AccessDeniedException", "NotAuthorized", "AccessDenied"} {
		t.Run(code, func(t *testing.T) {
			client := &fakeParameterGroupReader{err: apiErr(code)}
			err := checkDescribeDBParametersAccess(context.Background(), client, "my-pg")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "rds:DescribeDBParameters")
			assert.Contains(t, err.Error(), "my-pg")
		})
	}
}

func TestCheckDescribeDBParametersAccess_SeparatesCredentialFailures(t *testing.T) {
	// A broken token must not be reported as a missing IAM permission: the
	// two have completely different fixes.
	for _, code := range []string{
		"ExpiredTokenException",
		"UnrecognizedClientException",
		"MissingAuthenticationToken",
		"IncompleteSignature",
		"RequestExpired",
	} {
		t.Run(code, func(t *testing.T) {
			client := &fakeParameterGroupReader{err: apiErr(code)}
			err := checkDescribeDBParametersAccess(context.Background(), client, "my-pg")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "rejected the agent's credentials")
			assert.NotContains(t, err.Error(), "missing rds:DescribeDBParameters")
		})
	}
}

func TestCheckDescribeDBParametersAccess_SurfacesOtherFailures(t *testing.T) {
	// Not an authorization problem, so it must not claim to be one -- but it
	// still fails, matching the FetchDBInfo probe right above it.
	for name, readErr := range map[string]error{
		"missing group": &rdsTypes.DBParameterGroupNotFoundFault{},
		"network":       errors.New("dial tcp: i/o timeout"),
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeParameterGroupReader{err: readErr}
			err := checkDescribeDBParametersAccess(context.Background(), client, "my-pg")
			require.Error(t, err)
			assert.Contains(t, err.Error(), `failed to read parameter group "my-pg"`)
			assert.NotContains(t, err.Error(), "missing rds:DescribeDBParameters")
		})
	}
}

func TestIsAccessDenied(t *testing.T) {
	assert.True(t, isAccessDenied(apiErr("AccessDeniedException")))
	assert.True(t, isAccessDenied(apiErr("NotAuthorized")))
	// Documented RDS errors that are not authorization failures.
	assert.False(t, isAccessDenied(apiErr("ThrottlingException")))
	assert.False(t, isAccessDenied(apiErr("OptInRequired")))
	assert.False(t, isAccessDenied(apiErr("ExpiredTokenException")))
	assert.False(t, isAccessDenied(errors.New("boom")))
	assert.False(t, isAccessDenied(nil))
}

func TestIsCredentialError(t *testing.T) {
	assert.True(t, isCredentialError(apiErr("ExpiredTokenException")))
	assert.True(t, isCredentialError(apiErr("UnrecognizedClientException")))
	assert.False(t, isCredentialError(apiErr("AccessDeniedException")))
	assert.False(t, isCredentialError(errors.New("boom")))
	assert.False(t, isCredentialError(nil))
}
