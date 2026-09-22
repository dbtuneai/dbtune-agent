package rds

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/smithy-go"
)

type parameterGroupReader interface {
	DescribeDBParameters(
		ctx context.Context,
		in *rds.DescribeDBParametersInput,
		optFns ...func(*rds.Options),
	) (*rds.DescribeDBParametersOutput, error)
}

func checkDescribeDBParametersAccess(
	ctx context.Context,
	client parameterGroupReader,
	parameterGroupName string,
) error {
	_, err := client.DescribeDBParameters(ctx, &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(parameterGroupName),
		MaxRecords:           aws.Int32(20),
	})
	switch {
	case err == nil:
		return nil
	case isAccessDenied(err):
		return fmt.Errorf(
			"missing rds:DescribeDBParameters access on parameter group %q: This additional "+
				"requirement was added to allow better monitoring during config updates: %w",
			parameterGroupName, err)
	case isCredentialError(err):
		return fmt.Errorf(
			"AWS rejected the agent's credentials while reading parameter group %q: %w",
			parameterGroupName, err)
	default:
		return fmt.Errorf("failed to read parameter group %q: %w", parameterGroupName, err)
	}
}

// RDS models 145 error types in the SDK, none of them authorization failures,
// so an IAM denial arrives as an unmodeled smithy.GenericAPIError and has to be
// matched on its code string. The codes below are taken from the RDS API
// reference's Common Error Types page:
// https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/CommonErrors.html

// isAccessDenied reports whether AWS authenticated the request and then refused
// it on IAM policy grounds. The fix is a policy change.
func isAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "AccessDeniedException",
		"NotAuthorized",
		"AccessDenied":
		return true
	}
	return false
}

// isCredentialError reports whether the request was never authenticated in the
// first place.
func isCredentialError(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "ExpiredTokenException",
		"UnrecognizedClientException",
		"MissingAuthenticationToken",
		"IncompleteSignature",
		"RequestExpired":
		return true
	}
	return false
}
