package com.thesis.flinkapp.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DataCleanerJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        final String bootstrapServers = JobRuntime.envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        final String groupId = JobRuntime.envOrDefault("KAFKA_GROUP_ID", "flink-cleanse-v1");
        final String rawTopic = JobRuntime.envOrDefault("TOPIC_RAW", "credit_txn_raw");
        final String cleanTopic = JobRuntime.envOrDefault("TOPIC_CLEAN", "credit_txn_clean");
        final String dlqTopic = JobRuntime.envOrDefault("TOPIC_DLQ", "credit_txn_dlq");
        final String offsetMode = JobRuntime.envOrDefault("KAFKA_STARTING_OFFSETS", "committed");
        final String committedOffsetResetMode = JobRuntime.envOrDefault("KAFKA_COMMITTED_OFFSET_RESET", "latest");
        final String kafkaTxnPrefix = JobRuntime.envOrDefault("KAFKA_TXN_PREFIX", "cleaner-v1");
        final String kafkaTxnTimeoutMs = JobRuntime.envOrDefault("KAFKA_TX_TIMEOUT_MS", "600000");
        final boolean dropDeleteEvents = Boolean.parseBoolean(JobRuntime.envOrDefault("DROP_DELETE_EVENTS", "true"));
        final long checkpointMs = JobRuntime.envLongOrDefault("CHECKPOINT_INTERVAL_MS", 10000L);
        final long dedupTtlHours = JobRuntime.envLongOrDefault("DEDUP_TTL_HOURS", 24L);
        final long maxOutOfOrderSec = JobRuntime.envLongOrDefault("WATERMARK_OUT_OF_ORDER_SEC", 5L);
        final boolean writeBronzeParquet = Boolean.parseBoolean(JobRuntime.envOrDefault("WRITE_BRONZE_PARQUET", "true"));
        final boolean writeSilverParquet = Boolean.parseBoolean(JobRuntime.envOrDefault("WRITE_SILVER_PARQUET", "true"));
        final String bronzePath = JobRuntime.envOrDefault("BRONZE_PATH", "s3a://credit-fraud-bronze/txn_clean");
        final String silverPath = JobRuntime.envOrDefault("SILVER_PATH", "s3a://credit-fraud-silver/txn_clean");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointMs);

        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(rawTopic)
                .setGroupId(groupId)
                .setStartingOffsets(JobRuntime.resolveOffsets(offsetMode, committedOffsetResetMode))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final KafkaSink<String> cleanSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(kafkaTxnPrefix + "-clean")
                .setProperty("transaction.timeout.ms", kafkaTxnTimeoutMs)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(cleanTopic)
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

        DataStream<RoutedRecord> routed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-source")
                // implement cleaning and routing to clean vs dlq in one step to avoid multiple parsing of the same raw record
                .map(raw -> cleanDebeziumRecord(raw, dropDeleteEvents))
                .name("clean-and-route");

        DataStream<String> preDlq = routed
                .map(r -> r.dlq)
                .filter(v -> v != null)
                .name("pre-clean-dlq");

        DataStream<String> cleanCandidates = routed
                .map(r -> r.ok)
                .filter(v -> v != null)
                .name("clean-candidates");

        DataStream<String> cleanWithWatermarks = cleanCandidates
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderSec))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (json, recordTs) -> extractEventTsMs(json, recordTs))
                )
                .name("clean-watermarks");

        OutputTag<String> dedupDlqTag = new OutputTag<String>("cleaner-dedup-dlq") {
        };

        SingleOutputStreamOperator<String> uniqueClean = cleanWithWatermarks
                .keyBy(DataCleanerJob::extractTxnId)
                .process(new DedupByTxnIdProcess(dedupTtlHours, dedupDlqTag))
                .name("dedup-by-txn-id");

        DataStream<String> dedupDlq = uniqueClean.getSideOutput(dedupDlqTag);

        uniqueClean.sinkTo(cleanSink).name("kafka-clean-sink");
        preDlq.union(dedupDlq).sinkTo(dlqSink).name("kafka-dlq-sink");
        if (writeBronzeParquet) {
            uniqueClean
                    .map(DataCleanerJob::toBronzeParquet)
                    .filter(v -> v != null)
                    .addSink(buildBronzeParquetSink(bronzePath))
                    .name("bronze-parquet-sink");
        }
        if (writeSilverParquet) {
            uniqueClean
                    .map(DataCleanerJob::toBronzeParquet)
                    .filter(v -> v != null)
                    .addSink(buildBronzeParquetSink(silverPath))
                    .name("silver-parquet-sink");
        }

        env.execute("Credit Txn Data Cleaner Job");
    }

    private static RoutedRecord cleanDebeziumRecord(String rawJson, boolean dropDeleteEvents) {
        try {
            // clean job
            JsonNode root = MAPPER.readTree(rawJson);
            JsonNode payload = root.get("payload");
            if (payload == null || payload.isNull()) {
                return RoutedRecord.dlq(toDlq("cleaner", "MISSING_PAYLOAD", rawJson, null));
            }

            String op = text(payload, "op", "");
            JsonNode row = payload.get("after");
            if (row == null || row.isNull()) {
                row = payload.get("before");
            }
            if (row == null || row.isNull()) {
                return RoutedRecord.dlq(toDlq("cleaner", "MISSING_BEFORE_AFTER", rawJson, null));
            }
            if (dropDeleteEvents && "d".equals(op)) {
                return RoutedRecord.dlq(toDlq("cleaner", "DROP_DELETE_EVENT", rawJson, text(row, "txn_id", "unknown")));
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

            long txnId = cleaned.get("txn_id").asLong();
            if (txnId <= 0L) {
                return RoutedRecord.dlq(toDlq("cleaner", "INVALID_TXN_ID", rawJson, null));
            }
            return RoutedRecord.ok(MAPPER.writeValueAsString(cleaned));
        } catch (Exception ex) {
            return RoutedRecord.dlq(toDlq("cleaner", "PARSE_ERROR:" + ex.getClass().getSimpleName(), rawJson, null));
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

    private static long extractTxnId(String cleanedJson) {
        try {
            return MAPPER.readTree(cleanedJson).path("txn_id").asLong(-1L);
        } catch (Exception ignored) {
            return -1L;
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

    private static StreamingFileSink<BronzeTxnParquet> buildBronzeParquetSink(String path) {
        return StreamingFileSink
                .forBulkFormat(new Path(path), ParquetAvroWriters.forReflectRecord(BronzeTxnParquet.class))
                .withBucketAssigner(new DateTimeBucketAssigner<BronzeTxnParquet>("'dt='yyyy-MM-dd/HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();
    }

    private static BronzeTxnParquet toBronzeParquet(String cleanedJson) {
        try {
            JsonNode row = MAPPER.readTree(cleanedJson);
            BronzeTxnParquet out = new BronzeTxnParquet();
            out.txn_id = longVal(row, "txn_id", 0L);
            out.event_time_s = doubleVal(row, "event_time_s", 0.0d);
            out.v1 = doubleVal(row, "v1", 0.0d);
            out.v2 = doubleVal(row, "v2", 0.0d);
            out.v3 = doubleVal(row, "v3", 0.0d);
            out.v4 = doubleVal(row, "v4", 0.0d);
            out.v5 = doubleVal(row, "v5", 0.0d);
            out.v6 = doubleVal(row, "v6", 0.0d);
            out.v7 = doubleVal(row, "v7", 0.0d);
            out.v8 = doubleVal(row, "v8", 0.0d);
            out.v9 = doubleVal(row, "v9", 0.0d);
            out.v10 = doubleVal(row, "v10", 0.0d);
            out.v11 = doubleVal(row, "v11", 0.0d);
            out.v12 = doubleVal(row, "v12", 0.0d);
            out.v13 = doubleVal(row, "v13", 0.0d);
            out.v14 = doubleVal(row, "v14", 0.0d);
            out.v15 = doubleVal(row, "v15", 0.0d);
            out.v16 = doubleVal(row, "v16", 0.0d);
            out.v17 = doubleVal(row, "v17", 0.0d);
            out.v18 = doubleVal(row, "v18", 0.0d);
            out.v19 = doubleVal(row, "v19", 0.0d);
            out.v20 = doubleVal(row, "v20", 0.0d);
            out.v21 = doubleVal(row, "v21", 0.0d);
            out.v22 = doubleVal(row, "v22", 0.0d);
            out.v23 = doubleVal(row, "v23", 0.0d);
            out.v24 = doubleVal(row, "v24", 0.0d);
            out.v25 = doubleVal(row, "v25", 0.0d);
            out.v26 = doubleVal(row, "v26", 0.0d);
            out.v27 = doubleVal(row, "v27", 0.0d);
            out.v28 = doubleVal(row, "v28", 0.0d);
            out.amount = doubleVal(row, "amount", 0.0d);
            out.class_label = intVal(row, "class", 0);
            out.op = text(row, "op", "");
            out.event_ts_ms = longVal(row, "event_ts_ms", 0L);
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

    private static final class DedupByTxnIdProcess extends KeyedProcessFunction<Long, String, String> {
        private final long ttlHours;
        private final OutputTag<String> dlqTag;
        private transient ValueState<Boolean> seenState;

        private DedupByTxnIdProcess(long ttlHours, OutputTag<String> dlqTag) {
            this.ttlHours = ttlHours;
            this.dlqTag = dlqTag;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(Math.max(1L, ttlHours)))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-txn", Boolean.class);
            descriptor.enableTimeToLive(ttlConfig);
            seenState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            Boolean seen = seenState.value();
            if (Boolean.TRUE.equals(seen)) {
                ctx.output(dlqTag, toDlq("cleaner", "DUPLICATE_TXN_ID", value, String.valueOf(ctx.getCurrentKey())));
                return;
            }
            seenState.update(Boolean.TRUE);
            out.collect(value);
        }
    }

    public static final class BronzeTxnParquet {
        public long txn_id;
        public double event_time_s;
        public double v1;
        public double v2;
        public double v3;
        public double v4;
        public double v5;
        public double v6;
        public double v7;
        public double v8;
        public double v9;
        public double v10;
        public double v11;
        public double v12;
        public double v13;
        public double v14;
        public double v15;
        public double v16;
        public double v17;
        public double v18;
        public double v19;
        public double v20;
        public double v21;
        public double v22;
        public double v23;
        public double v24;
        public double v25;
        public double v26;
        public double v27;
        public double v28;
        public double amount;
        public int class_label;
        public String op;
        public long event_ts_ms;
    }
}
