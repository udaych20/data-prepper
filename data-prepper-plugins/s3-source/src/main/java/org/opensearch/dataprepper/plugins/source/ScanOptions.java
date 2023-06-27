/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Class consists the scan related properties.
 */
public class ScanOptions {

    private LocalDateTime startDateTime;

    private Duration range;

    private S3ScanBucketOption bucketOption;

    private LocalDateTime endDateTime;

    private LocalDateTime useStartDateTime;

    private LocalDateTime useEndDateTime;

    private ScanOptions(Builder builder){
        this.startDateTime = builder.startDateTime;
        this.range = builder.range;
        this.bucketOption = builder.bucketOption;
        this.endDateTime = builder.endDateTime;
        this.useStartDateTime = builder.useStartDateTime;
        this.useEndDateTime = builder.useEndDateTime;
    }

    public S3ScanBucketOption getBucketOption() {
        return bucketOption;
    }

    public LocalDateTime getUseStartDateTime() {
        return useStartDateTime;
    }

    public LocalDateTime getUseEndDateTime() {
        return useEndDateTime;
    }

    @Override
    public String toString() {
        return "startDateTime=" + startDateTime +
                ", range=" + range +
                ", endDateTime=" + endDateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder{

        private LocalDateTime startDateTime;

        private Duration range;

        private S3ScanBucketOption bucketOption;

        private LocalDateTime endDateTime;

        private LocalDateTime useStartDateTime;
        private LocalDateTime useEndDateTime;

        public Builder setStartDateTime(LocalDateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        public Builder setRange(Duration range) {
            this.range = range;
            return this;
        }

        public Builder setEndDateTime(LocalDateTime endDateTime) {
            this.endDateTime = endDateTime;
            return this;
        }

        public Builder setBucketOption(S3ScanBucketOption bucketOption) {
            this.bucketOption = bucketOption;
            return this;
        }

        public ScanOptions build() {
            LocalDateTime bucketStartDateTime = Objects.isNull(bucketOption.getStartTime()) ? startDateTime : bucketOption.getStartTime();
            LocalDateTime bucketEndDateTime = Objects.isNull(bucketOption.getEndTime()) ? endDateTime : bucketOption.getEndTime();
            Duration bucketRange = Objects.isNull(bucketOption.getRange()) ? range : bucketOption.getRange();

            long nonNullCount = Stream.of(bucketStartDateTime, bucketEndDateTime, bucketRange)
                    .filter(Objects::nonNull)
                    .count();

            if (nonNullCount == 0 || nonNullCount == 2) {
                setDateTimeToUse(bucketStartDateTime, bucketEndDateTime, bucketRange);
            } else if (nonNullCount == 3) {
                long originalBucketLevelNonNullCount = Stream.of(
                        bucketOption.getStartTime(), bucketOption.getEndTime(), bucketOption.getRange())
                        .filter(Objects::nonNull)
                        .count();

                if (originalBucketLevelNonNullCount == 2) {
                    setDateTimeToUse(bucketOption.getStartTime(), bucketOption.getEndTime(), bucketOption.getRange());
                } else {
                    scanRangeDateValidationError();
                }
            } else {
                scanRangeDateValidationError();
            }
            return new ScanOptions(this);
        }

        private void setDateTimeToUse(LocalDateTime bucketStartDateTime, LocalDateTime bucketEndDateTime, Duration bucketRange) {

            if (Objects.nonNull(bucketStartDateTime) && Objects.nonNull(bucketEndDateTime)) {
                this.useStartDateTime = bucketStartDateTime;
                this.useEndDateTime = bucketEndDateTime;
            } else if (Objects.nonNull(bucketStartDateTime) && Objects.nonNull(bucketRange)) {
                this.useStartDateTime = bucketStartDateTime;
                this.useEndDateTime = bucketStartDateTime.plus(bucketRange);
            } else if (Objects.nonNull(bucketEndDateTime) && Objects.nonNull(bucketRange)) {
                this.useStartDateTime = bucketEndDateTime.minus(bucketRange);
                this.useEndDateTime = bucketEndDateTime;
            }
        }

        private void scanRangeDateValidationError(){
            String message = "To set a time range for the bucket with name " + bucketOption.getName() +
                    ", specify any two configurations from start_time, end_time and range";
            throw new IllegalArgumentException(message);
        }

        @Override
        public String toString() {
            return "startDateTime=" + startDateTime +
                    ", range=" + range +
                    ", endDateTime=" + endDateTime;
        }
    }
}