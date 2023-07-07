/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.accumulator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.equalTo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(MockitoExtension.class)
class LocalFileBufferTest {

    public static final String KEY = UUID.randomUUID().toString() + ".log";
    public static final String PREFIX = "local";
    public static final String SUFFIX = ".log";

    private LocalFileBuffer localFileBuffer;
    private File tempFile;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile(PREFIX, SUFFIX);
        localFileBuffer = new LocalFileBuffer(tempFile);
    }

    @Test
    void test_with_write_events_into_buffer() throws IOException {
        while (localFileBuffer.getEventCount() < 55) {
            localFileBuffer.writeEvent(generateByteArray());
        }
        assertThat(localFileBuffer.getSize(), greaterThan(1l));
        assertThat(localFileBuffer.getEventCount(), equalTo(55));
        assertThat(localFileBuffer.getDuration(), equalTo(0L));
        localFileBuffer.flushAndCloseStream();
        localFileBuffer.removeTemporaryFile();
        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @Test
    void test_without_write_events_into_buffer() {
        assertThat(localFileBuffer.getSize(), equalTo(0L));
        assertThat(localFileBuffer.getEventCount(), equalTo(0));
        assertThat(localFileBuffer.getDuration(), equalTo(0L));
        localFileBuffer.flushAndCloseStream();
        localFileBuffer.removeTemporaryFile();
        assertFalse(tempFile.exists(), "The temp file has not been deleted.");
    }

    @Test
    void test_getSinkData_success() throws IOException{
        Assertions.assertNotNull(localFileBuffer);
        assertDoesNotThrow(() -> {
            localFileBuffer.getSinkBufferData();
        });
    }

    @AfterEach
    void cleanup() {
        tempFile.deleteOnExit();
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}