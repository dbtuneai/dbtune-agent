package rds

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/pi"
	rdsclient "github.com/aws/aws-sdk-go-v2/service/rds"
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
