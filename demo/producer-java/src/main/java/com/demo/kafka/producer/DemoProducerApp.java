package com.demo.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DemoProducerApp {

    public static void main(String[] args) throws Exception {
        Args cfg = Args.parse(args);

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int sent = 0;
            while (true) {
                String id = UUID.randomUUID().toString();
                String payload = "{\"id\":\"" + id + "\",\"message\":\"" + escape(cfg.message)
                        + "\",\"sent_at\":\"" + Instant.now() + "\",\"source\":\"producer-java\"}";

                ProducerRecord<String, String> record = new ProducerRecord<>(cfg.topic, id, payload);
                producer.send(record).get();
                sent++;
                System.out.println("sent #" + sent + " => " + payload);

                if (!cfg.loop && sent >= cfg.count) {
                    break;
                }
                Thread.sleep(cfg.intervalMs);
            }
        }
    }

    private static String escape(String input) {
        return input.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class Args {
        final String bootstrap;
        final String topic;
        final String message;
        final int count;
        final long intervalMs;
        final boolean loop;

        private Args(String bootstrap, String topic, String message, int count, long intervalMs, boolean loop) {
            this.bootstrap = bootstrap;
            this.topic = topic;
            this.message = message;
            this.count = count;
            this.intervalMs = intervalMs;
            this.loop = loop;
        }

        static Args parse(String[] args) {
            String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:39092");
            String topic = envOrDefault("KAFKA_TOPIC", "demo_simple_events");
            String message = "hello from producer-java";
            int count = 1;
            long intervalMs = 1000L;
            boolean loop = false;

            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if ("--bootstrap".equals(a) && i + 1 < args.length) {
                    bootstrap = args[++i];
                } else if ("--topic".equals(a) && i + 1 < args.length) {
                    topic = args[++i];
                } else if ("--message".equals(a) && i + 1 < args.length) {
                    message = args[++i];
                } else if ("--count".equals(a) && i + 1 < args.length) {
                    count = Integer.parseInt(args[++i]);
                } else if ("--interval-ms".equals(a) && i + 1 < args.length) {
                    intervalMs = Long.parseLong(args[++i]);
                } else if ("--loop".equals(a)) {
                    loop = true;
                }
            }

            return new Args(bootstrap, topic, message, count, intervalMs, loop);
        }

        private static String envOrDefault(String key, String defaultValue) {
            String value = System.getenv(key);
            return value == null || value.trim().isEmpty() ? defaultValue : value;
        }
    }
}
