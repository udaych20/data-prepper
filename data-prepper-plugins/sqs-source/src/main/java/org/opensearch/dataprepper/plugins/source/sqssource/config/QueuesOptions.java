/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Positive;

import java.time.Duration;
import java.util.List;

/**
 *  read the sqs queue configuration from pipeline
 */
public class QueuesOptions {

    @JsonProperty("urls")
    private List<String> urls;

    @JsonProperty("polling_frequency")
    private Duration pollingFrequency = Duration.ZERO;

    @JsonProperty("batch_size")
    private Integer batchSize;

    @JsonProperty("number_of_threads")
    @Positive(message = "number_of_threads should be unsigned value")
    private int numberOfThreads = 1;

    public List<String> getUrls() {
        return urls;
    }

    public Duration getPollingFrequency() {
        return pollingFrequency;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

}
