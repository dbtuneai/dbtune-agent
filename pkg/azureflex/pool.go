package azureflex

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	AuthMethodPostgres = "postgres"
	AuthMethodEntra    = "entra"

	entraTokenScope = "https://ossrdbms-aad.database.windows.net/.default" //nolint:gosec // public OAuth scope
)

// NewPool creates a pgxpool for Azure Flex Server. When AuthMethod is "entra",
// a BeforeConnect hook fetches a fresh Azure AD token and injects it as the password.
func NewPool(ctx context.Context, connectionURL string, cfg Config) (*pgxpool.Pool, error) {
	if cfg.AuthMethod != AuthMethodEntra {
		return pgxpool.New(ctx, connectionURL)
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("entra auth: failed to create Azure credential: %w", err)
	}

	poolCfg, err := pgxpool.ParseConfig(connectionURL)
	if err != nil {
		return nil, fmt.Errorf("entra auth: failed to parse connection URL: %w", err)
	}

	poolCfg.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
		token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
			Scopes: []string{entraTokenScope},
		})
		if err != nil {
			return fmt.Errorf("entra auth: failed to get token: %w", err)
		}
		connCfg.Password = token.Token
		return nil
	}

	return pgxpool.NewWithConfig(ctx, poolCfg)
}
