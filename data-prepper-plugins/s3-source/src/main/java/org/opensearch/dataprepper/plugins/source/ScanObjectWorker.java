/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Class responsible for processing the s3 scan objects with the help of <code>S3ObjectWorker</code>
 * or <code>S3SelectWorker</code>.
 */
public class ScanObjectWorker implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ScanObjectWorker.class);
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    static final char DAYS = 'd';
    static final char WEEKS = 'w';
    static final char MONTHS = 'm';
    static final char YEARS = 'y';
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final Buffer<Record<Event>> buffer;
    private final int numberOfRecordsToAccumulate;
    private final Duration bufferTimeout;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final BiConsumer<Event, S3ObjectReference> eventMetadataModifier;
    private static final Map<String,S3ObjectDetails> stateSaveMap = new HashMap<>();
    private final List<ScanOptions> scanOptionsBuilderList;

    private S3ObjectHandler s3ObjectHandler;

    public ScanObjectWorker(final S3ObjectRequest s3ObjectRequest,
                            final List<ScanOptions> scanOptionsBuilderList,
                            final S3ObjectHandler s3ObjectHandler){
        this.s3Client = s3ObjectRequest.getS3Client();
        this.s3AsyncClient = s3ObjectRequest.getS3AsyncClient();
        this.scanOptionsBuilderList = scanOptionsBuilderList;
        this.buffer = s3ObjectRequest.getBuffer();
        this.s3ObjectPluginMetrics = s3ObjectRequest.getS3ObjectPluginMetrics();
        this.numberOfRecordsToAccumulate = s3ObjectRequest.getNumberOfRecordsToAccumulate();
        this.bufferTimeout = s3ObjectRequest.getBufferTimeout();
        this.eventMetadataModifier = s3ObjectRequest.getEventConsumer();
        this.bucketOwnerProvider = s3ObjectRequest.getBucketOwnerProvider();
        this.s3ObjectHandler= s3ObjectHandler;
    }

    /**
     * It will decide the s3 object parse <code>S3ObjectWorker</code> or <code>S3SelectWorker</code>
     * based on s3 select configuration provided.
     */
    @Override
    public void run() {
        scanOptionsBuilderList.forEach(this::parseS3ScanObjects);
    }

    /**
     * Method will parse the s3 object and write to {@link Buffer}
     */
    void parseS3ScanObjects(final ScanOptions scanOptions) {
        scanOptions.getIncludeKeyPaths().forEach(key ->{
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(scanOptions.getBucket())
                    .prefix(key).build();

            final ListObjectsV2Response s3ObjectResponse = s3Client.listObjectsV2(request);
            s3ObjectResponse.contents().stream().map(S3Object::key).filter(s3ObjKey-> s3ObjKey.lastIndexOf(".")!=-1)
                    .filter(s -> !scanOptions.getExcludeKeyPaths().contains(s.substring(s.lastIndexOf("."))))
                    .forEach(s3Object -> {
                        processS3ObjectKeys(S3ObjectReference.bucketAndKey(scanOptions.getBucket(),
                                s3Object).build(),s3ObjectHandler, scanOptions);
                    });
        });
    }


    private void processS3ObjectKeys(final S3ObjectReference s3ObjectReference,
                                     final S3ObjectHandler s3ObjectHandler,
                                     final ScanOptions scanOptionsBuilder){
        final S3ObjectDetails s3ObjDetails = getS3ObjectDetails(s3ObjectReference);
        final boolean isKeyMatchedBetweenTimeRange = isKeyMatchedBetweenTimeRange(s3ObjDetails.getS3ObjectLastModifiedTimestamp(),
                scanOptionsBuilder.getRange(),
                LocalDateTime.parse(scanOptionsBuilder.getStartDate()));
        if(isKeyMatchedBetweenTimeRange && (isKeyProcessedByS3Scan(s3ObjDetails))){
            updateKeyProcessedByS3Scan(s3ObjDetails);
            try{
                s3ObjectHandler.parseS3Object(s3ObjectReference,null);
            }catch (IOException ex){
                deleteKeyProcessedByS3Scan(s3ObjDetails);
                LOG.error("Error while process the parseS3Object. ",ex);
            }
        }
    }
    /**
     * Method will identify already processed key.
     * @return boolean
     */
    public boolean isKeyProcessedByS3Scan(final S3ObjectDetails s3ObjectDetails) {
        return stateSaveMap.get(s3ObjectDetails.getBucket()+s3ObjectDetails.getKey()) == null;
    }

    /**
     * store the processed bucket key in the map.
     */
    public void updateKeyProcessedByS3Scan(final S3ObjectDetails s3ObjectDetails) {
        stateSaveMap.put((s3ObjectDetails.getBucket() + s3ObjectDetails.getKey()),s3ObjectDetails);
    }
    private void deleteKeyProcessedByS3Scan(S3ObjectDetails s3ObjDetails) {
        stateSaveMap.remove(s3ObjDetails.getBucket() + s3ObjDetails.getKey());
    }

    /**
     * fetch the s3 object last modified time.
     * @return S3ObjectDetails
     */
    public S3ObjectDetails getS3ObjectDetails(final S3ObjectReference s3ObjectReference){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(s3ObjectReference.getBucketName()).key(s3ObjectReference.getKey()).build();
        ResponseInputStream<GetObjectResponse> s3ObjectResp = s3Client.getObject(getObjectRequest);
        final Instant instant = s3ObjectResp.response().lastModified();
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
        return new S3ObjectDetails(s3ObjectReference.getBucketName(),s3ObjectReference.getKey(),zonedDateTime.toLocalDateTime());
    }

    /**
     * used for identifying the slurping start date based on the range string.
     * @return LocalDateTime
     */
    public LocalDateTime getSlurpingStartDateByRange(final String rangeString,final LocalDateTime startDateTime){
        final LocalDateTime slurpingDateTime;
        int rangeValue = Integer.parseInt(rangeString.substring(0, rangeString.length() - 1));
        char unit = rangeString.charAt(rangeString.length() - 1);
        switch(unit){
            case DAYS:
                slurpingDateTime = startDateTime.minusDays(rangeValue);
                break;
            case WEEKS:
                slurpingDateTime =  startDateTime.minusWeeks(rangeValue);
                break;
            case MONTHS:
                slurpingDateTime = startDateTime.minusMonths(rangeValue);
                break;
            case YEARS:
                slurpingDateTime = startDateTime.minusYears(rangeValue);
                break;
            default:
                throw new IllegalArgumentException("Invalid time unit: " + unit);
        }
        return slurpingDateTime;
    }

    /**
     * Used for identifying s3 object last modified time match with slurping date range.
     * @return boolean
     */
    public boolean isKeyMatchedBetweenTimeRange(final LocalDateTime lastModifiedTime,
                                                final String rangeString,
                                                final LocalDateTime endDate){
        return lastModifiedTime.isAfter(getSlurpingStartDateByRange(rangeString, endDate))
                && lastModifiedTime.isBefore(endDate);
    }
}
