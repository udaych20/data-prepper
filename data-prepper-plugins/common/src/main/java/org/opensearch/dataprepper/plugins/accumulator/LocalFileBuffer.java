/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold local file data and flushing it to S3.
 */
public class LocalFileBuffer implements Buffer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBuffer.class);
    private final OutputStream outputStream;
    private int eventCount;
    private final StopWatch watch;
    private final File localFile;

    LocalFileBuffer(File tempFile) throws FileNotFoundException {
        localFile = tempFile;
        outputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            LOG.error("An exception occurred while flushing data to buffered output stream :", e);
        }
        return localFile.length();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    @Override
    public long getDuration(){
        return watch.getTime(TimeUnit.SECONDS);
    }

    @Override
    public byte[] getSinkBufferData() throws IOException {
        final byte[] fileData = Files.readAllBytes(localFile.toPath());
        removeTemporaryFile();
        return fileData;
    }

    /**
     * write byte array to output stream.
     * @param bytes byte array.
     * @throws IOException while writing to output stream fails.
     */
    @Override
    public void writeEvent(byte[] bytes) throws IOException {
        outputStream.write(bytes);
        outputStream.write(System.lineSeparator().getBytes());
        eventCount++;
    }

    /**
     * Flushing the buffered data into the output stream.
     */
    protected void flushAndCloseStream(){
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.error("An exception occurred while flushing data to buffered output stream :", e);
        }
    }

    /**
     * Remove the local temp file after flushing data to s3.
     */
    protected void removeTemporaryFile() {
        if (localFile != null) {
            try {
                Files.deleteIfExists(Paths.get(localFile.toString()));
            } catch (IOException e) {
                LOG.error("Unable to delete Local file {}", localFile, e);
            }
        }
    }
}