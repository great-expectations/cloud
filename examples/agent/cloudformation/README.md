# Deploying the GX Agent via CloudFormation

1. Ensure you have the aws cli tool installed and access to the AWS account you will be deploying to. You will need access to ECS, iam and secrets manager.

2. Go into AWS secret manager and create a new secret. This secret will hold the following two keys `GX_CLOUD_ACCESS_TOKEN` and `GX_CLOUD_ORGANIZATION_ID`.

- The Organization ID and token can be found and generated in GX Cloud under Settings > Tokens.
- Copy the ARN of the secret and save this for the `parameters.json`` file later

3. Go into the AWS console and find the subnet group and security group that you would like the GX Agent container to use. Copy the resource id for each of these, they should take a form similar to `sg-abcd1234` and `subnet-abcd1234`.

4. Open up a terminal and create a file named `parameters.json`. In the parameters.json file paste the following and add in your arn and resource values that you copied from the previous steps.

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

5. Create the cloudformation stack

```
aws cloudformation create-stack --stack-name gx-agent --template-url https://gx-agent-cloudformation.s3.amazonaws.com/agent-stack.yaml --parameters file://parameters.json --capabilities CAPABILITY_IAM
```

6. Go into the AWS console under the CloudFormation page to see that the gx-agent has been spun up. Then go to ECS to see your task running, in the logs you should see that the agent has spun up.

7. In a browser go to GX Cloud to start running validation and fetching metrics.

# For Devs

Upload the cloudformation template to s3
`aws s3 cp agent-stack.yaml <s3://s3-bucket/path-to-stack-file>`
