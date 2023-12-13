# Deploying the GX Agent via CloudFormation

1. Ensure you have the AWS CLI tool installed and access to the AWS account you will be deploying to. You will need access to ECS, IAM, and Secrets Manager.

2. Go into AWS Secrets Manager and create a new secret. This secret will hold the following two keys: `GX_CLOUD_ACCESS_TOKEN` and `GX_CLOUD_ORGANIZATION_ID`.

- The Organization ID and token can be found and generated in GX Cloud under Settings > Tokens.
- Copy the ARN of the secret and save this for the `parameters.json` file later

3. Go into the AWS Management Console and find the subnet group and security group that you would like the GX Agent container to use. Copy the resource ID for each of these; they should use a format similar to `sg-abcd1234` or `subnet-abcd1234`.

4. Open up a terminal and create a file named `parameters.json`. In the `parameters.json` file, paste the following and add in your ARN and resource values that you copied from the previous steps.

```
[
  {
    "ParameterKey": "TaskSecurityGroup",
    "ParameterValue": "sg-abcd1234"
  },
  {
    "ParameterKey": "TaskSubnetGroup",
    "ParameterValue": "subnet-abcd1234"
  },
  {
    "ParameterKey": "GXAgentSecretARN",
    "ParameterValue": "arn:aws:secretsmanager:us-east-1:012345678910:secret:/ecs/gx-agent-credentials-abc123"
  }
]
```

5. Create the CloudFormation stack

```
aws cloudformation create-stack --stack-name gx-agent --template-url https://gx-agent-cloudformation.s3.amazonaws.com/agent-stack.yaml --parameters file://parameters.json --capabilities CAPABILITY_IAM
```

6. Go into the AWS Management Console under the CloudFormation page to see that the GX Agent has been spun up. Then go to ECS to see your task running--in the logs you should see that the GX Agent has been spun up.

7. In a browser, go to GX Cloud to start running Checkpoints and fetching metrics.

# For Devs

Upload the CloudFormation template to s3
`aws s3 cp agent-stack.yaml <s3://s3-bucket/path-to-stack-file>`
