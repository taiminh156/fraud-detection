package com.thesis.flinkapp.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FeatureEngineeringJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        final String bootstrapServers = JobRuntime.envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        final String groupId = JobRuntime.envOrDefault("KAFKA_GROUP_ID", "flink-features-v1");
        final String cleanTopic = JobRuntime.envOrDefault("TOPIC_CLEAN", "credit_txn_clean");
        final String featuresTopic = JobRuntime.envOrDefault("TOPIC_FEATURES", "credit_txn_features");
        final String offsetMode = JobRuntime.envOrDefault("KAFKA_STARTING_OFFSETS", "earliest");
        final long checkpointMs = JobRuntime.envLongOrDefault("CHECKPOINT_INTERVAL_MS", 10000L);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointMs);

        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(cleanTopic)
                .setGroupId(groupId)
                .setStartingOffsets(JobRuntime.resolveOffsets(offsetMode))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(featuresTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-clean-source")
                .map(FeatureEngineeringJob::buildFeatures)
                .filter(feature -> feature != null)
                .sinkTo(sink)
                .name("kafka-feature-sink");

        env.execute("Credit Txn Feature Engineering Job");
    }

    private static String buildFeatures(String cleanedJson) {
        try {
            JsonNode row = MAPPER.readTree(cleanedJson);
            long txnId = longVal(row, "txn_id", 0L);
            if (txnId <= 0L) {
                return null;
            }

            double amount = doubleVal(row, "amount", 0.0d);
            double absVSum = 0.0d;
            double absVMax = 0.0d;
            for (int i = 1; i <= 28; i++) {
                double v = Math.abs(doubleVal(row, "v" + i, 0.0d));
                absVSum += v;
                absVMax = Math.max(absVMax, v);
            }
            double absVMean = absVSum / 28.0d;

            ObjectNode out = MAPPER.createObjectNode();
            out.put("txn_id", txnId);
            out.put("event_ts_ms", longVal(row, "event_ts_ms", 0L));
            out.put("event_time_s", doubleVal(row, "event_time_s", 0.0d));
            out.put("amount", amount);
            out.put("class", intVal(row, "class", 0));
            out.put("op", text(row, "op", ""));

            out.put("amount_log1p", Math.log1p(Math.max(amount, 0.0d)));
            out.put("amount_is_zero", amount == 0.0d ? 1 : 0);
            out.put("abs_v_mean", absVMean);
            out.put("abs_v_max", absVMax);
            out.put("high_risk_signal", absVMax >= 10.0d ? 1 : 0);

            return MAPPER.writeValueAsString(out);
        } catch (Exception ignored) {
            return null;
        }
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
