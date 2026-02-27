package com.thesis.flinkapp.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.time.Duration;

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
        final String dlqTopic = JobRuntime.envOrDefault("TOPIC_DLQ", "credit_txn_dlq");
        final String offsetMode = JobRuntime.envOrDefault("KAFKA_STARTING_OFFSETS", "committed");
        final String committedOffsetResetMode = JobRuntime.envOrDefault("KAFKA_COMMITTED_OFFSET_RESET", "latest");
        final String kafkaTxnPrefix = JobRuntime.envOrDefault("KAFKA_TXN_PREFIX", "features-v1");
        final String kafkaTxnTimeoutMs = JobRuntime.envOrDefault("KAFKA_TX_TIMEOUT_MS", "600000");
        final long checkpointMs = JobRuntime.envLongOrDefault("CHECKPOINT_INTERVAL_MS", 10000L);
        final long maxOutOfOrderSec = JobRuntime.envLongOrDefault("WATERMARK_OUT_OF_ORDER_SEC", 5L);
        final boolean writeGoldParquet = Boolean.parseBoolean(JobRuntime.envOrDefault("WRITE_GOLD_PARQUET", "true"));
        final String goldPath = JobRuntime.envOrDefault("GOLD_PATH", "s3a://credit-fraud-gold/txn_features");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointMs);
        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(cleanTopic)
                .setGroupId(groupId)
                .setStartingOffsets(JobRuntime.resolveOffsets(offsetMode, committedOffsetResetMode))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final KafkaSink<String> featureSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(kafkaTxnPrefix + "-features")
                .setProperty("transaction.timeout.ms", kafkaTxnTimeoutMs)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(featuresTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        final KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(kafkaTxnPrefix + "-dlq")
                .setProperty("transaction.timeout.ms", kafkaTxnTimeoutMs)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(dlqTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        DataStream<String> cleanWithWatermarks = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-clean-source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderSec))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) FeatureEngineeringJob::extractEventTsMs)
                )
                .name("feature-watermarks");

        DataStream<RoutedRecord> routed = cleanWithWatermarks
                .map(FeatureEngineeringJob::buildFeatures)
                .name("build-features-and-route");

        routed.map(r -> r.ok)
                .filter(v -> v != null)
                .sinkTo(featureSink)
                .name("kafka-feature-sink");

        routed.map(r -> r.dlq)
                .filter(v -> v != null)
                .sinkTo(dlqSink)
                .name("kafka-feature-dlq-sink");
        if (writeGoldParquet) {
            routed.map(r -> r.ok)
                    .filter(v -> v != null)
                    .map(FeatureEngineeringJob::toGoldParquet)
                    .filter(v -> v != null)
                    .addSink(buildGoldParquetSink(goldPath))
                    .name("gold-parquet-sink");
        }

        env.execute("Credit Txn Feature Engineering Job");
    }

    private static RoutedRecord buildFeatures(String cleanedJson) {
        try {
            JsonNode row = MAPPER.readTree(cleanedJson);
            long txnId = longVal(row, "txn_id", 0L);
            if (txnId <= 0L) {
                return RoutedRecord.dlq(toDlq("feature", "INVALID_TXN_ID", cleanedJson, null));
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
            // Bắt được oulier tốt hơn khi dùng max
            out.put("abs_v_max", absVMax);
            // Gia tri bat thường > 10
            out.put("high_risk_signal", absVMax >= 10.0d ? 1 : 0);

            return RoutedRecord.ok(MAPPER.writeValueAsString(out));
        } catch (Exception ex) {
            return RoutedRecord.dlq(toDlq("feature", "PARSE_OR_FEATURE_ERROR:" + ex.getClass().getSimpleName(), cleanedJson, null));
        }
    }

    private static long extractEventTsMs(String cleanedJson, long fallback) {
        try {
            long ts = MAPPER.readTree(cleanedJson).path("event_ts_ms").asLong(0L);
            return ts > 0 ? ts : fallback;
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static String toDlq(String stage, String reason, String raw, String txnId) {
        try {
            ObjectNode dlq = MAPPER.createObjectNode();
            dlq.put("stage", stage);
            dlq.put("reason", reason);
            if (txnId != null) {
                dlq.put("txn_id", txnId);
            }
            dlq.put("ts_ms", System.currentTimeMillis());
            dlq.put("raw", raw);
            return MAPPER.writeValueAsString(dlq);
        } catch (Exception ignored) {
            return "{\"stage\":\"" + stage + "\",\"reason\":\"" + reason + "\"}";
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

    private static StreamingFileSink<GoldTxnParquet> buildGoldParquetSink(String path) {
        return StreamingFileSink
                .forBulkFormat(new Path(path), ParquetAvroWriters.forReflectRecord(GoldTxnParquet.class))
                .withBucketAssigner(new DateTimeBucketAssigner<GoldTxnParquet>("'dt='yyyy-MM-dd/HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
    }

    private static GoldTxnParquet toGoldParquet(String featureJson) {
        try {
            JsonNode row = MAPPER.readTree(featureJson);
            GoldTxnParquet out = new GoldTxnParquet();
            out.txn_id = longVal(row, "txn_id", 0L);
            out.event_ts_ms = longVal(row, "event_ts_ms", 0L);
            out.event_time_s = doubleVal(row, "event_time_s", 0.0d);
            out.amount = doubleVal(row, "amount", 0.0d);
            out.class_label = intVal(row, "class", 0);
            out.op = text(row, "op", "");
            out.amount_log1p = doubleVal(row, "amount_log1p", 0.0d);
            out.amount_is_zero = intVal(row, "amount_is_zero", 0);
            out.abs_v_mean = doubleVal(row, "abs_v_mean", 0.0d);
            out.abs_v_max = doubleVal(row, "abs_v_max", 0.0d);
            out.high_risk_signal = intVal(row, "high_risk_signal", 0);
            return out;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static final class RoutedRecord {
        final String ok;
        final String dlq;

        private RoutedRecord(String ok, String dlq) {
            this.ok = ok;
            this.dlq = dlq;
        }

        static RoutedRecord ok(String value) {
            return new RoutedRecord(value, null);
        }

        static RoutedRecord dlq(String value) {
            return new RoutedRecord(null, value);
        }
    }

    public static final class GoldTxnParquet {
        public long txn_id;
        public long event_ts_ms;
        public double event_time_s;
        public double amount;
        public int class_label;
        public String op;
        public double amount_log1p;
        public int amount_is_zero;
        public double abs_v_mean;
        public double abs_v_max;
        public int high_risk_signal;
    }
}
