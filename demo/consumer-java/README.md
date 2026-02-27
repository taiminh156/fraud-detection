# Consumer Java

## Run
```bash
cd demo/consumer-java
mvn -q -DskipTests package
mvn -q exec:java
```

## Options
- `--bootstrap` (default `localhost:39092`)
- `--topic` (default `demo_simple_events`)
- `--group` (default `demo-consumer-java-v1`)
- `--offset-reset` (default `earliest`)
