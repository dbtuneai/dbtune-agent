package rds

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
)

// rdsEndpointSuffix is the DNS suffix AWS gives every RDS and Aurora endpoint.
// A configured host carrying it is addressing RDS directly and can therefore be
// checked against the endpoints AWS reports for the instance. A host without it
// (RDS Proxy, PgBouncer, an SSH tunnel, a CNAME) cannot be judged either way
// without resolving DNS, which the agent does not do.
const rdsEndpointSuffix = ".rds.amazonaws.com"

// EndpointCheck is the outcome of comparing the Postgres host the agent reads
// settings from against the endpoints of the instance whose parameter group it
// writes to.
type EndpointCheck int

const (
	// EndpointVerified means the configured host is one of the instance's own
	// AWS endpoints, so reads and writes provably address the same instance.
	EndpointVerified EndpointCheck = iota
	// EndpointMismatch means the configured host is an RDS endpoint that does
	// not belong to the tuned instance: the agent would read one database's
	// settings while writing another instance's parameter group.
	EndpointMismatch
	// EndpointUnverifiable means the host could not be matched either way,
	// because it is not an RDS endpoint or because AWS reported no endpoints
	// for the instance yet.
	EndpointUnverifiable
)

// KnownEndpoints returns every AWS-reported DNS name that addresses this
// instance: its own endpoint plus, for Aurora, its cluster's writer, reader and
// custom endpoints. Connecting through any of them tunes this instance, so all
// are treated as equivalent.
func (info *DBInfo) KnownEndpoints() []string {
	endpoints := make([]string, 0, 1+len(info.ClusterEndpoints))
	if info.InstanceEndpoint != "" {
		endpoints = append(endpoints, info.InstanceEndpoint)
	}
	endpoints = append(endpoints, info.ClusterEndpoints...)
	return endpoints
}

// VerifyConnectionEndpoint reports whether the Postgres connection the agent
// reads settings from actually addresses the RDS instance whose parameter group
// it writes to.
//
// The two halves are configured independently — postgresql.connection_url and
// rds.RDS_DATABASE_IDENTIFIER — and nothing else ties them together. A typo or
// a config copied between databases leaves the agent reading one instance's
// pg_settings while applying changes to another instance's parameter group.
// Both halves work in isolation, so the mismatch is otherwise silent.
//
// A mismatch is logged, not fatal: connecting through RDS Proxy, PgBouncer, an
// SSH tunnel or a CNAME is legitimate and cannot be distinguished from a typo
// without DNS resolution. The outcome is returned so a caller that wants to be
// stricter can act on it.
func VerifyConnectionEndpoint(
	dbInfo *DBInfo,
	connectionURL string,
	logger *logrus.Logger,
) EndpointCheck {
	if dbInfo == nil {
		return EndpointUnverifiable
	}
	databaseIdentifier := aws.ToString(dbInfo.DBInstance.DBInstanceIdentifier)

	hosts, err := connectionHosts(connectionURL)
	if err != nil {
		// pgxpool.New would already have failed on an unparseable URL, so this
		// is close to unreachable; warn rather than fail the check outright.
		logger.Warnf("Could not verify the Postgres connection targets instance %q: %v", databaseIdentifier, err)
		return EndpointUnverifiable
	}

	known := dbInfo.KnownEndpoints()
	if len(known) == 0 {
		logger.Warnf(
			"AWS reported no endpoints for instance %q, cannot verify the Postgres connection targets it",
			databaseIdentifier,
		)
		return EndpointUnverifiable
	}

	outcome, host := classifyEndpoints(hosts, known)
	switch outcome {
	case EndpointVerified:
		logger.Infof("Postgres host %q is an endpoint of instance %q", host, databaseIdentifier)
	case EndpointMismatch:
		logger.Warnf(
			"Postgres host %q is an RDS endpoint but not one of instance %q (known endpoints: %s). "+
				"The agent reads settings over this connection and applies changes to the parameter group "+
				"of %q, so it may be tuning a different database than it measures. "+
				"Check postgresql.connection_url and rds.RDS_DATABASE_IDENTIFIER.",
			host, databaseIdentifier, strings.Join(known, ", "), databaseIdentifier,
		)
	case EndpointUnverifiable:
		logger.Infof(
			"Postgres host %q is not an RDS endpoint (proxy, tunnel or CNAME); "+
				"cannot verify it targets instance %q, whose parameter group the agent writes to",
			host, databaseIdentifier,
		)
	}
	return outcome
}

// classifyEndpoints matches the hosts pgx would dial against the instance's
// known endpoints. A single verified host is enough, since pgx connects to one
// of them; a mismatch is only reported when no host matched. The returned host
// is the one the outcome is about, for use in the log message.
func classifyEndpoints(hosts []string, known []string) (EndpointCheck, string) {
	normalizedKnown := make(map[string]struct{}, len(known))
	for _, endpoint := range known {
		normalizedKnown[normalizeHost(endpoint)] = struct{}{}
	}

	var mismatch string
	for _, host := range hosts {
		normalized := normalizeHost(host)
		if _, ok := normalizedKnown[normalized]; ok {
			return EndpointVerified, host
		}
		if mismatch == "" && strings.HasSuffix(normalized, rdsEndpointSuffix) {
			mismatch = host
		}
	}

	if mismatch != "" {
		return EndpointMismatch, mismatch
	}
	if len(hosts) > 0 {
		return EndpointUnverifiable, hosts[0]
	}
	return EndpointUnverifiable, ""
}

// normalizeHost puts a DNS name in the form used for comparison. Hostnames are
// case-insensitive and may carry a fully-qualifying trailing dot.
func normalizeHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
}

// connectionHosts returns the hosts pgx would dial for connectionURL, in the
// order it tries them, de-duplicated. Both URL and keyword/DSN forms are
// accepted. Only hosts are returned; the rest of the connection string carries
// the password and must not be logged.
func connectionHosts(connectionURL string) ([]string, error) {
	config, err := pgconn.ParseConfig(connectionURL)
	if err != nil {
		// pgconn redacts the password from ParseConfigError, so wrapping the
		// error does not leak the credentials in the connection string.
		return nil, fmt.Errorf("failed to parse Postgres connection URL: %w", err)
	}

	// pgx records one fallback per host, and additionally per TLS mode when
	// sslmode allows a downgrade, so the same host recurs; de-duplicate.
	hosts := make([]string, 0, 1+len(config.Fallbacks))
	seen := make(map[string]struct{}, 1+len(config.Fallbacks))
	appendHost := func(host string) {
		if host == "" {
			return
		}
		normalized := normalizeHost(host)
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		hosts = append(hosts, host)
	}

	appendHost(config.Host)
	for _, fallback := range config.Fallbacks {
		appendHost(fallback.Host)
	}
	return hosts, nil
}
