package com.thesis.flinkapp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

public class DataStreamJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        final String groupId = envOrDefault("KAFKA_GROUP_ID", "flink-cleanse-v1");
        final String rawTopic = envOrDefault("TOPIC_RAW", "credit_txn_raw");
        final String cleanTopic = envOrDefault("TOPIC_CLEAN", "credit_txn_clean");
        final String offsetMode = envOrDefault("KAFKA_STARTING_OFFSETS", "earliest");
        final boolean dropDeleteEvents = Boolean.parseBoolean(envOrDefault("DROP_DELETE_EVENTS", "true"));
        final long checkpointMs = Long.parseLong(envOrDefault("CHECKPOINT_INTERVAL_MS", "10000"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointMs);

        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(rawTopic)
                .setGroupId(groupId)
                .setStartingOffsets(resolveOffsets(offsetMode))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(cleanTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-source")
                .map(raw -> cleanDebeziumRecord(raw, dropDeleteEvents))
                .filter(cleaned -> cleaned != null)
                .sinkTo(sink)
                .name("kafka-clean-sink");

        env.execute("Kafka Debezium Data Cleaner");
    }

    private static String cleanDebeziumRecord(String rawJson, boolean dropDeleteEvents) {
        try {
            JsonNode root = MAPPER.readTree(rawJson);
            JsonNode payload = root.get("payload");
            if (payload == null || payload.isNull()) {
                return null;
            }

            String op = text(payload, "op", "");
            JsonNode row = payload.get("after");
            if (row == null || row.isNull()) {
                row = payload.get("before");
            }
            if (row == null || row.isNull()) {
                return null;
            }
            if (dropDeleteEvents && "d".equals(op)) {
                return null;
            }

            ObjectNode cleaned = MAPPER.createObjectNode();
            cleaned.put("txn_id", longVal(row, "txn_id", 0L));
            cleaned.put("event_time_s", doubleVal(row, "event_time_s", 0.0d));
            for (int i = 1; i <= 28; i++) {
                String field = "v" + i;
                cleaned.put(field, doubleVal(row, field, 0.0d));
            }
            cleaned.put("amount", doubleVal(row, "amount", 0.0d));
            cleaned.put("class", intVal(row, "class", 0));
            cleaned.put("op", op);
            cleaned.put("event_ts_ms", longVal(payload, "ts_ms", 0L));

            if (cleaned.get("txn_id").asLong() <= 0L) {
                return null;
            }
            return MAPPER.writeValueAsString(cleaned);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static OffsetsInitializer resolveOffsets(String mode) {
        if ("latest".equalsIgnoreCase(mode)) {
            return OffsetsInitializer.latest();
        }
        return OffsetsInitializer.earliest();
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return StringUtils.isNullOrWhitespaceOnly(value) ? defaultValue : value;
    }

    private static String text(JsonNode node, String field, String defaultValue) {
        JsonNode child = node.get(field);
        return child == null || child.isNull() ? defaultValue : child.asText(defaultValue);
    }

    private static long longVal(JsonNode node, String field, long defaultValue) {
        JsonNode child = node.get(field);
        return child == null || child.isNull() ? defaultValue : child.asLong(defaultValue);
    }

    private static int intVal(JsonNode node, String field, int defaultValue) {
        JsonNode child = node.get(field);
        return child == null || child.isNull() ? defaultValue : child.asInt(defaultValue);
    }

    private static double doubleVal(JsonNode node, String field, double defaultValue) {
        JsonNode child = node.get(field);
        return child == null || child.isNull() ? defaultValue : child.asDouble(defaultValue);
    }
}
