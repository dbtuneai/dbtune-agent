package rds

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	instanceEndpoint = "prod-db.abc123.eu-north-1.rds.amazonaws.com"
	writerEndpoint   = "prod-cluster.cluster-abc123.eu-north-1.rds.amazonaws.com"
	readerEndpoint   = "prod-cluster.cluster-ro-abc123.eu-north-1.rds.amazonaws.com"
	customEndpoint   = "analytics.cluster-custom-abc123.eu-north-1.rds.amazonaws.com"
	otherInstance    = "staging-db.abc123.eu-north-1.rds.amazonaws.com"
)

func TestKnownEndpoints(t *testing.T) {
	t.Run("instance only", func(t *testing.T) {
		info := &DBInfo{InstanceEndpoint: instanceEndpoint}
		assert.Equal(t, []string{instanceEndpoint}, info.KnownEndpoints())
	})

	t.Run("instance and cluster", func(t *testing.T) {
		info := &DBInfo{
			InstanceEndpoint: instanceEndpoint,
			ClusterEndpoints: []string{writerEndpoint, readerEndpoint, customEndpoint},
		}
		assert.Equal(t, []string{instanceEndpoint, writerEndpoint, readerEndpoint, customEndpoint},
			info.KnownEndpoints())
	})

	t.Run("no endpoints reported", func(t *testing.T) {
		assert.Empty(t, (&DBInfo{}).KnownEndpoints())
	})
}

func TestClusterEndpoints(t *testing.T) {
	t.Run("collects writer reader and custom", func(t *testing.T) {
		cluster := &rdsTypes.DBCluster{
			Endpoint:        aws.String(writerEndpoint),
			ReaderEndpoint:  aws.String(readerEndpoint),
			CustomEndpoints: []string{customEndpoint},
		}
		assert.Equal(t, []string{writerEndpoint, readerEndpoint, customEndpoint},
			clusterEndpoints(cluster))
	})

	t.Run("skips absent and empty endpoints", func(t *testing.T) {
		cluster := &rdsTypes.DBCluster{
			Endpoint:        aws.String(writerEndpoint),
			CustomEndpoints: []string{""},
		}
		assert.Equal(t, []string{writerEndpoint}, clusterEndpoints(cluster))
	})
}

func TestClassifyEndpoints(t *testing.T) {
	known := []string{instanceEndpoint, writerEndpoint, readerEndpoint, customEndpoint}

	testCases := []struct {
		name  string
		hosts []string
		want  EndpointCheck
	}{
		{"instance endpoint", []string{instanceEndpoint}, EndpointVerified},
		{"cluster writer endpoint", []string{writerEndpoint}, EndpointVerified},
		{"cluster reader endpoint", []string{readerEndpoint}, EndpointVerified},
		{"aurora custom endpoint", []string{customEndpoint}, EndpointVerified},
		{"uppercase host", []string{"PROD-DB.ABC123.EU-NORTH-1.RDS.AMAZONAWS.COM"}, EndpointVerified},
		{"trailing dot host", []string{instanceEndpoint + "."}, EndpointVerified},

		// The failure this check exists for: a different RDS instance, which
		// happens to share the account's endpoint zone.
		{"different rds instance", []string{otherInstance}, EndpointMismatch},

		// Legitimate indirection: cannot be judged without resolving DNS.
		{"rds proxy", []string{"prod-proxy.proxy-abc123.eu-north-1.amazonaws.com"}, EndpointUnverifiable},
		{"ssh tunnel", []string{"localhost"}, EndpointUnverifiable},
		{"private ip", []string{"10.0.1.42"}, EndpointUnverifiable},
		{"cname", []string{"db.internal.example.com"}, EndpointUnverifiable},
		{"unix socket dir", []string{"/var/run/postgresql"}, EndpointUnverifiable},

		{"no hosts", nil, EndpointUnverifiable},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			outcome, _ := classifyEndpoints(testCase.hosts, known)
			assert.Equal(t, testCase.want, outcome)
		})
	}

	t.Run("one matching host among several is enough", func(t *testing.T) {
		outcome, host := classifyEndpoints([]string{otherInstance, instanceEndpoint}, known)
		assert.Equal(t, EndpointVerified, outcome)
		assert.Equal(t, instanceEndpoint, host)
	})

	t.Run("reports the mismatching host", func(t *testing.T) {
		outcome, host := classifyEndpoints([]string{"localhost", otherInstance}, known)
		assert.Equal(t, EndpointMismatch, outcome)
		assert.Equal(t, otherInstance, host)
	})
}

func TestConnectionHosts(t *testing.T) {
	t.Run("url form", func(t *testing.T) {
		hosts, err := connectionHosts("postgres://user:secret@" + instanceEndpoint + ":5432/postgres")
		require.NoError(t, err)
		assert.Equal(t, []string{instanceEndpoint}, hosts)
	})

	t.Run("dsn form", func(t *testing.T) {
		hosts, err := connectionHosts("host=" + instanceEndpoint + " user=u password=secret dbname=postgres")
		require.NoError(t, err)
		assert.Equal(t, []string{instanceEndpoint}, hosts)
	})

	t.Run("sslmode require does not duplicate the host", func(t *testing.T) {
		hosts, err := connectionHosts("postgres://user:secret@" + instanceEndpoint + ":5432/postgres?sslmode=require")
		require.NoError(t, err)
		assert.Equal(t, []string{instanceEndpoint}, hosts)
	})

	t.Run("sslmode prefer does not duplicate the host", func(t *testing.T) {
		hosts, err := connectionHosts("postgres://user:secret@" + instanceEndpoint + ":5432/postgres?sslmode=prefer")
		require.NoError(t, err)
		assert.Equal(t, []string{instanceEndpoint}, hosts)
	})

	t.Run("multiple hosts are all returned", func(t *testing.T) {
		hosts, err := connectionHosts("postgres://user:secret@" + instanceEndpoint + "," + otherInstance + ":5432/postgres?sslmode=require")
		require.NoError(t, err)
		assert.Equal(t, []string{instanceEndpoint, otherInstance}, hosts)
	})

	t.Run("unparseable url does not leak the password", func(t *testing.T) {
		_, err := connectionHosts("postgres://user:sup3rsecret@host:notaport/postgres")
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "sup3rsecret")
	})
}

func TestVerifyConnectionEndpoint(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel) // keep test output quiet

	dbInfo := func() *DBInfo {
		return &DBInfo{
			DBInstance:       rdsTypes.DBInstance{DBInstanceIdentifier: aws.String("prod-db")},
			InstanceEndpoint: instanceEndpoint,
		}
	}

	t.Run("matching endpoint verifies", func(t *testing.T) {
		got := VerifyConnectionEndpoint(dbInfo(),
			"postgres://user:secret@"+instanceEndpoint+":5432/postgres", logger)
		assert.Equal(t, EndpointVerified, got)
	})

	t.Run("different rds instance is a mismatch", func(t *testing.T) {
		got := VerifyConnectionEndpoint(dbInfo(),
			"postgres://user:secret@"+otherInstance+":5432/postgres", logger)
		assert.Equal(t, EndpointMismatch, got)
	})

	t.Run("proxy host is unverifiable", func(t *testing.T) {
		got := VerifyConnectionEndpoint(dbInfo(),
			"postgres://user:secret@db.internal.example.com:5432/postgres", logger)
		assert.Equal(t, EndpointUnverifiable, got)
	})

	t.Run("no reported endpoints is unverifiable", func(t *testing.T) {
		info := &DBInfo{DBInstance: rdsTypes.DBInstance{DBInstanceIdentifier: aws.String("prod-db")}}
		got := VerifyConnectionEndpoint(info,
			"postgres://user:secret@"+instanceEndpoint+":5432/postgres", logger)
		assert.Equal(t, EndpointUnverifiable, got)
	})

	t.Run("nil db info is unverifiable", func(t *testing.T) {
		assert.Equal(t, EndpointUnverifiable, VerifyConnectionEndpoint(nil, "", logger))
	})
}
