package rds

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isolateAWSEnv clears every input the default credential chain reads ahead of (or instead of) the
// container credentials endpoint, and points the shared config files at paths that don't exist, so
// the test never picks up the developer's own AWS setup.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"AWS_PROFILE",
		"AWS_DEFAULT_PROFILE",
		"AWS_ACCESS_KEY_ID",
		"AWS_ACCESS_KEY",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SECRET_KEY",
		"AWS_SESSION_TOKEN",
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_ROLE_ARN",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
	} {
		t.Setenv(name, "")
	}
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// The default credential chain on ECS/Fargate task roles (and EKS Pod Identity) fetches
// credentials from a container endpoint whose SDK client has no UserAgent build step. The PRM
// middleware is in the shared config's API options, so it is applied to that client too and must
// not fail there.
func TestFetchAWSConfig_ContainerCredentials(t *testing.T) {
	isolateAWSEnv(t)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"AccessKeyId": "AKIDCONTAINER",
			"SecretAccessKey": "container-secret",
			"Token": "container-token",
			"Expiration": "` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"
		}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL+"/creds")

	ctx := context.Background()
	cfg, err := FetchAWSConfig("", "", "us-east-1", ctx)
	require.NoError(t, err)

	creds, err := cfg.Credentials.Retrieve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "AKIDCONTAINER", creds.AccessKeyID)
	assert.Equal(t, "container-secret", creds.SecretAccessKey)
	assert.Equal(t, "container-token", creds.SessionToken)
	assert.EqualValues(t, 1, hits.Load(), "credentials should come from the container endpoint")
}

// Skipping stacks without a UserAgent step must not also drop the tag from service clients, and the
// token must reach the wire with its "/" intact.
func TestFetchAWSConfig_ServiceRequestsCarryPRMUserAgent(t *testing.T) {
	isolateAWSEnv(t)

	var gotUserAgent atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserAgent.Store(r.Header.Get("User-Agent"))
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte(`<DescribeDBInstancesResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
  <DescribeDBInstancesResult><DBInstances/></DescribeDBInstancesResult>
  <ResponseMetadata><RequestId>test</RequestId></ResponseMetadata>
</DescribeDBInstancesResponse>`))
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	cfg, err := FetchAWSConfig("AKIDSTATIC", "static-secret", "us-east-1", ctx)
	require.NoError(t, err)

	client := rds.NewFromConfig(cfg, func(o *rds.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
	_, err = client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})
	require.NoError(t, err)

	ua, _ := gotUserAgent.Load().(string)
	assert.Contains(t, ua, awsPartnerAttributionUserAgent)
}
