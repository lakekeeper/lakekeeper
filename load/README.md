1. terminal:

```
cd iceberg-rest-server/examples/minimal
docker compose -f docker-compose.yaml -f docker-compose-build.yaml up --build -d
docker run --network=host -v "./prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus
```

2. terminal

```
cd iceberg-rest-server/load
k6 run load.js
```
