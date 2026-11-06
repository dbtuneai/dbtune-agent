package rds

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	rdsclient "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go/middleware"
)

type AWSClients struct {
	EC2Client        *ec2.Client
	RDSClient        *rdsclient.Client
	PIClient         *pi.Client
	CloudwatchClient *cloudwatch.Client
}

func NewAWSClients(cfg aws.Config) AWSClients {
	ec2Client := ec2.NewFromConfig(cfg)
	rdsClient := rdsclient.NewFromConfig(cfg)
	piClient := pi.NewFromConfig(cfg)
	cloudwatchClient := cloudwatch.NewFromConfig(cfg)
	return AWSClients{
		EC2Client:        ec2Client,
		RDSClient:        rdsClient,
		PIClient:         piClient,
		CloudwatchClient: cloudwatchClient,
	}
}

// AWS PRM attribution token. Appended via raw header middleware below, not AddUserAgentKey(Value) -
// those sanitize "/" to "-" and corrupt this format (confirmed live via CloudTrail).
const awsPartnerAttributionUserAgent = "APN_1.1/pc_dml4zrdqnmoacdcn4u6vmz9q4$"

// Must run after the SDK's "UserAgent" middleware, or that middleware overwrites this.
type prmUserAgentMiddleware struct{}

func addPRMUserAgent(stack *middleware.Stack) error {
	return stack.Build.Insert(prmUserAgentMiddleware{}, "UserAgent", middleware.After)
}

func FetchAWSConfig(
	awsAccessKey string,
	awsSecretAccessKey string,
	awsRegion string,
	ctx context.Context,
) (aws.Config, error) {
	region := config.WithRegion(awsRegion)
	apiOptions := config.WithAPIOptions([]func(*middleware.Stack) error{
		addPRMUserAgent,
	})
	if awsAccessKey != "" && awsSecretAccessKey != "" {
		// Use static credentials if provided
		creds := credentials.NewStaticCredentialsProvider(awsAccessKey, awsSecretAccessKey, "")
		provider := config.WithCredentialsProvider(creds)
		return config.LoadDefaultConfig(ctx, region, provider, apiOptions)
	} else {
		// Use default credential chain
		// Includes by default WebIdentityToken:
		// https://github.com/aws/aws-sdk-go-v2/blob/main/config/resolve_credentials.go#L119
		return config.LoadDefaultConfig(ctx, region, apiOptions)
	}
}
