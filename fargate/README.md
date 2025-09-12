# DBTune Setup and Configuration

### Prerequisites
These instructions require that you have the AWS cli v2 installed and your `~/.aws/credentials` file configured for your AWS account. [AWS CLI Install Instructions.](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)


##These commands are a one-time setup.

###Prerequisites 

Gather the following information that will be specific to your deployment. Examples are provided in parenthesis.

- AWS\_ACCOUNT\_ID (`123445543331`)
- AWS_REGION (`us-east-1`)
- AWS\_SECURITY\_GROUPS (`sg-1556ec70`) (should be a comma separated list if you have multiple, this is an array in the commands below)
- AWS_SUBNETS (`subnet-55938821,subnet-27c9ed0f`) (should be a comma separated list if you have multiple, this is an array in the commands below)

The actual values that pertain to your AWS account need to be replaced in the commands below that are denoted by the `${}` convention.


### IAM Role and Policies

```
aws iam create-role --role-name dbTuneTaskRole \
  --description "DBTune Agent ECS tasks" \
  --assume-role-policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'

aws iam attach-role-policy --role-name dbTuneTaskRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

aws iam put-role-policy --role-name dbTuneTaskRole \
  --policy-name GetSSMPganalyzeParameters \
  --policy-document '{"Statement":[{"Action": ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"], "Effect": "Allow", "Resource": "arn:aws:ssm:*:*:parameter/dbtune/*"}, {"Action": "kms:Decrypt", "Effect": "Allow", "Resource": "arn:aws:kms:*:*:*"}]}'

aws iam attach-role-policy --role-name dbTuneTaskRole \
  --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/DBTune
```

### Configuring the agent on Amazon ECS

##### AWS SSM Configuration
The namespace conventions used below for SSM parameter names can be changed to your organizations requirements. This example assumes an _environment_ of `staging`.

```
aws ssm put-parameter --name /global/dbtune/api_key --type SecureString --value "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
aws ssm put-parameter --name /staging/dbtune/postgres_connection_url --type SecureString --value "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

#### ECS Cluster Creation

```
aws ecs create-cluster --cluster-name dbtune

```

---

#### Register the ECS Task Definition and Create Log group

**NOTE**: These commands are `environment` (or database) specific. If you are attempting to monitor multiple databases, you will need to run these commands for *each* database. This example represents a `staging` database.

The input file `dbtune_task.json` also needs to be updated with the variable replacements that pertain to your AWS Account. Make sure to edit/save this file before running the next command.

```
aws ecs register-task-definition --cli-input-json file://dbtune_task.json --no-cli-pager
aws logs create-log-group --log-group-name /ecs/dbtune/staging --no-cli-pager
```

**Launch as a Service:**

```
aws ecs create-service \
	--cluster dbtune \
	--service-name dbtune-fargate-staging \
	--task-definition dbtune-fargate-staging \
	--desired-count 1 \
	--launch-type FARGATE \
	--platform-version LATEST \
	--network-configuration "awsvpcConfiguration={assignPublicIp=ENABLED,subnets=[${AWS_SUBNETS}],securityGroups=[${AWS_SECURITY_GROUPS}]}" \
	--no-cli-pager
```

**Setup an alarm for monitoring**

Add the EventBridge Rule

```
aws events put-rule --name "dbtune-fargate-staging" --event-pattern "{\"source\":[\"aws.ecs\"],\"detail-type\":[\"ECS Task State Change\"],\"detail\":{\"lastStatus\":[\"STOPPED\"],\"clusterArn\":[\"arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:cluster/dbtune\"]}}" --state "ENABLED" --description "Monitoring rule for ECS DBTune staging" --event-bus-name "default" --no-cli-pager
```

Add the Target for the Rule

This command _assumes_ you have an SNS topic called `AlertDevopsEngineers`. Point this target rule to any SNS topic you want a message sent to that your team will get alerted for ECS task state changes.

```
aws events put-targets --rule dbtune-fargate-staging --targets "Id"="Target1","Arn"="arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:AlertDevopsEngineers" --no-cli-pager
```


## Update an Existing Task Definition and Redeploy
In the event that you need to change or fix an issue with the task definition, the following steps will allow you to update the ECS task definition and _update_ the service. 

**NOTE:** After running these commands, you will have two tasks running as the original is shutting down, be patient and it will terminate on it's own. You may see some duplicate metrics being sent to DBTune for a very short period until the original terminates and the newly updated task is Running.


- Update the task definition defined in the `dbtune_task.json` file.
- Run `aws ecs register-task-definition --cli-input-json file://dbtune_task.json --no-cli-page`
- Determine the **latest version** that gets provided in the output from the command in the previous step from the `taskDefinitionArn`
- Run `aws ecs update-service --cluster dbtune --service dbtune-fargate-staging --task-definition dbtune-fargate-staging:4 --force-new-deployment`
- Tail the logs (see below) and make sure you don't get any errors.

## Obtain shell access to a container task

Enable execute command for the service:
```
aws ecs update-service --cluster dbtune --service dbtune-fargate-staging --enable-execute-command --force-new-deployment
```

Obtain terminal shell to container:
```
aws ecs execute-command --cluster dbtune \
    --task ${ECS_TASK_ARN} \
    --container dbtune \
    --interactive \
    --command "/bin/sh" \
    --region ${AWS_REGION}
```


### ECS CLI Commands

List ECS Clusters

```
aws ecs list-clusters

aws ecs list-tasks --cluster dbtune

```

##### Log Monitoring

```
aws logs tail --follow /ecs/dbtune/staging
```
