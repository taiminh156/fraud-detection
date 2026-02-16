package com.thesis.flinkapp.jobs;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.StringUtils;

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
        if ("latest".equalsIgnoreCase(mode)) {
            return OffsetsInitializer.latest();
        }
        return OffsetsInitializer.earliest();
    }
}
