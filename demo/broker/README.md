# Kafka Demo Broker

Broker này chạy độc lập với đồ án fraud detection.

## Start
```bash
cd demo/broker
docker compose up -d
```

## Endpoints
- Kafka bootstrap (host): `localhost:39092`
- Kafka UI: `http://localhost:8088`
- Demo topic: `demo_simple_events`

## Stop
```bash
docker compose down
```
