package pg

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	AuthMethodPassword = "password"
	AuthMethodEntra    = "entra"

	// entraTokenScope is the OAuth scope for Azure Database for PostgreSQL.
	entraTokenScope = "https://ossrdbms-aad.database.windows.net/.default"
)

// NewPool creates a pgxpool from the given Config.
// When AuthMethod is "entra", a BeforeConnect hook is installed that fetches a
// fresh Azure AD token via DefaultAzureCredential and injects it as the
// connection password. This allows connecting to Azure Flex Server using
// Microsoft Entra authentication without a static password.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	if cfg.AuthMethod != AuthMethodEntra {
		return pgxpool.New(ctx, cfg.ConnectionURL)
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("entra auth: failed to create Azure credential: %w", err)
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.ConnectionURL)
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
