package com.thesis.flinkapp.jobs;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

final class JobRuntime {

    private JobRuntime() {
    }

    static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return StringUtils.isNullOrWhitespaceOnly(value) ? defaultValue : value;
    }

    static long envLongOrDefault(String key, long defaultValue) {
        String raw = envOrDefault(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw);
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    static OffsetsInitializer resolveOffsets(String mode) {
        return resolveOffsets(mode, "latest");
    }

    static OffsetsInitializer resolveOffsets(String mode, String committedOffsetResetMode) {
        if ("latest".equalsIgnoreCase(mode)) {
            return OffsetsInitializer.latest();
        }
        if ("committed".equalsIgnoreCase(mode)) {
            return OffsetsInitializer.committedOffsets(resolveResetStrategy(committedOffsetResetMode));
        }
        return OffsetsInitializer.earliest();
    }

    private static OffsetResetStrategy resolveResetStrategy(String mode) {
        if ("earliest".equalsIgnoreCase(mode)) {
            return OffsetResetStrategy.EARLIEST;
        }
        return OffsetResetStrategy.LATEST;
    }
}
