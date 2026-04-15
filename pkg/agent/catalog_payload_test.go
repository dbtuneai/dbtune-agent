package agent

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtuneai/agent/pkg/dbtune"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendCatalogPayload(t *testing.T) {
	payload := []byte(`{"rows":[{"a":1}]}`)

	var gotHeaders http.Header
	var gotBody []byte
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		gotPath = r.URL.Path
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = b
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ca := &CommonAgent{
		logger:    logrus.New(),
		APIClient: retryablehttp.NewClient(),
		ServerURLs: dbtune.ServerURLs{
			ServerUrl: server.URL,
			ApiKey:    "k",
			DbID:      "db",
		},
		StartTime: time.Now().UTC().Format(time.RFC3339),
	}
	ca.APIClient.Logger = nil

	err := ca.SendCatalogPayload(context.Background(), "pg_stats", payload)
	require.NoError(t, err)

	assert.Equal(t, "gzip", gotHeaders.Get("Content-Encoding"))
	assert.Equal(t, "application/json", gotHeaders.Get("Content-Type"))
	assert.True(t, strings.Contains(gotPath, "pg_stats"), "path should include collector name, got %q", gotPath)

	gr, err := gzip.NewReader(strings.NewReader(string(gotBody)))
	require.NoError(t, err)
	decoded, err := io.ReadAll(gr)
	require.NoError(t, err)
	assert.Equal(t, payload, decoded)
}

func TestSendCatalogPayload_NonSuccessStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad input"))
	}))
	defer server.Close()

	client := retryablehttp.NewClient()
	client.RetryMax = 0
	ca := &CommonAgent{
		logger:    logrus.New(),
		APIClient: client,
		ServerURLs: dbtune.ServerURLs{
			ServerUrl: server.URL,
			ApiKey:    "k",
			DbID:      "db",
		},
	}

	err := ca.SendCatalogPayload(context.Background(), "pg_stats", []byte(`{}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}
