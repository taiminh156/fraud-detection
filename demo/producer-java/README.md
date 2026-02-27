# Producer Java

## Run
```bash
cd demo/producer-java
mvn -q -DskipTests package
mvn -q exec:java -Dexec.args="--message hello --count 3"
```

## Options
- `--bootstrap` (default `localhost:39092`)
- `--topic` (default `demo_simple_events`)
- `--message`
- `--count`
- `--interval-ms`
- `--loop`
