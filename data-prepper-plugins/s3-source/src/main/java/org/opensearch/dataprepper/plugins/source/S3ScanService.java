/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Class responsible for taking an {@link S3SourceConfig} and creating all the necessary {@link ScanOptions}
 * objects and spawn a thread {@link S3SelectObjectWorker}
 */
public class S3ScanService {
    private final List<S3ScanBucketOptions> s3ScanBucketOptions;
    private final Buffer<Record<Event>> buffer;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final S3ClientBuilderFactory s3ClientBuilderFactory;
    private final String endTime;
    private final String range;
    private CompressionOption compressionOption;

    private S3ObjectHandler s3ObjectHandler;

    public S3ScanService(final S3SourceConfig s3SourceConfig,
                         final Buffer<Record<Event>> buffer,
                         final BucketOwnerProvider bucketOwnerProvider,
                         final BiConsumer<Event, S3ObjectReference> eventConsumer,
                         final S3ObjectPluginMetrics s3ObjectPluginMetrics,
                         final S3ClientBuilderFactory s3ClientBuilderFactory,
                         final S3ObjectHandler s3ObjectHandler) {
        this.s3ScanBucketOptions = s3SourceConfig.getS3ScanScanOptions().getBuckets();
        this.buffer = buffer;
        this.numberOfRecordsToAccumulate = s3SourceConfig.getNumberOfRecordsToAccumulate();
        this.bufferTimeout = s3SourceConfig.getBufferTimeout();
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.eventConsumer = eventConsumer;
        this.s3ObjectPluginMetrics = s3ObjectPluginMetrics;
        this.s3ClientBuilderFactory = s3ClientBuilderFactory;
        this.endTime = s3SourceConfig.getS3ScanScanOptions().getStartTime();
        this.range = s3SourceConfig.getS3ScanScanOptions().getRange();
        this.compressionOption = s3SourceConfig.getCompression();
        this.s3ObjectHandler = s3ObjectHandler;
    }

    public void start() {
        S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer, numberOfRecordsToAccumulate, bufferTimeout, s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3AsyncClient(s3ClientBuilderFactory.getS3AsyncClient())
                .s3Client(s3ClientBuilderFactory.getS3Client())
                .eventConsumer(eventConsumer).build();
        Thread t = new Thread(new ScanObjectWorker(s3ObjectRequest, getScanOptions(),s3ObjectHandler));
        t.start();
    }

    /**
     * This Method Used to fetch the scan options details from {@link S3SourceConfig} amd build the
     * all the s3 scan buckets information in list.
     *
     * @return @List<ScanOptionsBuilder>
     */
    List<ScanOptions> getScanOptions() {
        List<ScanOptions> scanOptionsList = new ArrayList<>();
        s3ScanBucketOptions.forEach(
                obj -> {
                    buildScanOptions(scanOptionsList, obj);
                });
        return scanOptionsList;
    }

    private void buildScanOptions(final List<ScanOptions> scanOptionsList, final S3ScanBucketOptions scanBucketOptions) {
        final S3ScanBucketOption bucket = scanBucketOptions.getS3ScanBucketOption();
        scanOptionsList.add(new ScanOptions()
                .setStartDate(endTime).setRange(range)
                .setBucket(bucket.getName())
                .setIncludeKeyPaths(bucket.getKeyPath().getS3scanIncludeOptions())
                .setExcludeKeyPaths(bucket.getKeyPath().getS3ScanExcludeOptions()));
//                .setCompressionOption(bucket.getCompression() != null ? bucket.getCompression() : compressionOption));
    }
}
