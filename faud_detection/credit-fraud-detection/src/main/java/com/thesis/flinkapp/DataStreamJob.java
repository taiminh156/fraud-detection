package com.thesis.flinkapp;

import com.thesis.flinkapp.jobs.DataCleanerJob;
import com.thesis.flinkapp.jobs.FeatureEngineeringJob;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        String selectedJob = args != null && args.length > 0 ? args[0] : System.getenv("JOB_NAME");
        if (selectedJob == null || selectedJob.trim().isEmpty()) {
            selectedJob = "cleaner";
        }

        if ("cleaner".equalsIgnoreCase(selectedJob) || "data-cleaner".equalsIgnoreCase(selectedJob)) {
            DataCleanerJob.run();
            return;
        }

        if ("feature".equalsIgnoreCase(selectedJob) || "features".equalsIgnoreCase(selectedJob)
                || "feature-engineering".equalsIgnoreCase(selectedJob)) {
            FeatureEngineeringJob.run();
            return;
        }

        throw new IllegalArgumentException(
                "Unknown JOB_NAME='" + selectedJob
                        + "'. Supported values: cleaner, feature"
        );
    }
}
