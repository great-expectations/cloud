# How to deploy agent with Docker

## Prepare your secrets

```shell
cat > secrets.txt << EOF
GX_CLOUD_ORGANIZATION_ID=
GX_CLOUD_ACCESS_TOKEN=
GX_CLOUD_SNOWFLAKE_PW=
EOF
```

## Run agent and pass in secrets

```shell
docker run -it \
--env-file=secrets.txt \
greatexpectations/agent:latest
```

## Use our image as a base image

### Example Dockerfile

```Dockerfile
FROM greatexpectations/agent

RUN echo "add your commands like pip install here"
```

### Build Docker image

```shell
docker build -t myorg/agent .
```
