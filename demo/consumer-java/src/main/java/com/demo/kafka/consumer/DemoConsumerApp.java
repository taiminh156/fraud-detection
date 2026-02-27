package com.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DemoConsumerApp {

    public static void main(String[] args) {
        Args cfg = Args.parse(args);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cfg.offsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(cfg.topic));
            System.out.println("consuming topic=" + cfg.topic + " bootstrap=" + cfg.bootstrap + " group=" + cfg.groupId);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.println(
                            "topic=" + r.topic()
                                    + " partition=" + r.partition()
                                    + " offset=" + r.offset()
                                    + " key=" + r.key()
                                    + " value=" + r.value()
                    );
                }
            }
        }
    }

    private static final class Args {
        final String bootstrap;
        final String topic;
        final String groupId;
        final String offsetReset;

        private Args(String bootstrap, String topic, String groupId, String offsetReset) {
            this.bootstrap = bootstrap;
            this.topic = topic;
            this.groupId = groupId;
            this.offsetReset = offsetReset;
        }

        static Args parse(String[] args) {
            String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:39092");
            String topic = envOrDefault("KAFKA_TOPIC", "demo_simple_events");
            String groupId = envOrDefault("KAFKA_GROUP", "demo-consumer-java-v1");
            String offsetReset = envOrDefault("KAFKA_OFFSET_RESET", "earliest");

            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if ("--bootstrap".equals(a) && i + 1 < args.length) {
                    bootstrap = args[++i];
                } else if ("--topic".equals(a) && i + 1 < args.length) {
                    topic = args[++i];
                } else if ("--group".equals(a) && i + 1 < args.length) {
                    groupId = args[++i];
                } else if ("--offset-reset".equals(a) && i + 1 < args.length) {
                    offsetReset = args[++i];
                }
            }

            return new Args(bootstrap, topic, groupId, offsetReset);
        }

        private static String envOrDefault(String key, String defaultValue) {
            String value = System.getenv(key);
            return value == null || value.trim().isEmpty() ? defaultValue : value;
        }
    }
}
