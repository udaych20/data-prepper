/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold in memory data and flushing it to S3.
 */
public class InMemoryBuffer implements Buffer {

    private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private int eventCount;
    private final StopWatch watch;

    InMemoryBuffer() {
        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        return byteArrayOutputStream.size();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }
    @Override
    public OutputStream getOutputStream() {
        return byteArrayOutputStream;
    }

    public long getDuration() {
        return watch.getTime(TimeUnit.SECONDS);
    }

    /**
     * Upload accumulated data to s3 bucket.
     *
     * @param s3Client s3 client object.
     * @param bucket   bucket name.
     * @param key      s3 object key path.
     */
    @Override
    public void flushToS3(S3Client s3Client, String bucket, String key) {
        final byte[] byteArray = byteArrayOutputStream.toByteArray();
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromBytes(byteArray));
    }


}