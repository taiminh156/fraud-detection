# Demo Kafka (Java)

Toàn bộ demo nằm trong thư mục `demo/`, bạn có thể xóa cả thư mục này bất cứ lúc nào.

## 1) Start broker (dùng lại bộ broker cũ)
```bash
cd demo/broker
docker compose up -d
```

- Bootstrap host: `localhost:39092`
- Kafka UI: `http://localhost:8088`
- Topic: `demo_simple_events`

## 2) Run consumer Java
```bash
cd demo/consumer-java
mvn -q -DskipTests package
mvn -q exec:java
```

## 3) Run producer Java
```bash
cd demo/producer-java
mvn -q -DskipTests package
mvn -q exec:java -Dexec.args="--message hello-demo --count 5"
```

## 4) Cleanup
```bash
cd demo/broker
docker compose down
```

Sau đó nếu muốn xóa toàn bộ demo:
```bash
cd ..
rm -rf demo
```
