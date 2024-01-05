# Deploy agent in kubernetes

## Create kubernetes secret

Provide any secrets the container needs to operate with the name of the secret then the value

```shell
# Add your secrets after the =
kubectl create secret generic gx-agent-secret \
    --from-literal=GX_CLOUD_ORGANIZATION_ID= \
    --from-literal=GX_CLOUD_ACCESS_TOKEN= \
    --from-literal=GX_CLOUD_SNOWFLAKE_PW=
```

## Create deployemnt

```shell
kubectl apply -f deployment.yaml
```


## Clean up

```shell
kubectl delete -f deployment.yaml
kubectl delete secret gx-agent-secret
```
