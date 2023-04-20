/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.util.List;
/**
 * Class consists the scan related properties.
 */
public class ScanOptions {
    private String startDate;
    private String range;
    private String bucket;
    private String expression;
    private S3SelectSerializationFormatOption serializationFormatOption;
    private CompressionOption compressionOption;

    private CompressionType compressionType;

    private S3SelectCSVOption s3SelectCSVOption;

    private S3SelectJsonOption s3SelectJsonOption;

    private String expressionType;

    private List<String> includeKeyPaths;

    private List<String> excludeKeyPaths;

    public String getExpressionType() {
        return expressionType;
    }

    public ScanOptions setExpressionType(String expressionType) {
        this.expressionType = expressionType;
        return this;
    }

    public S3SelectCSVOption getS3SelectCSVOption() {
        return s3SelectCSVOption;
    }

    public ScanOptions setS3SelectCSVOption(S3SelectCSVOption s3SelectCSVOption) {
        this.s3SelectCSVOption = s3SelectCSVOption;
        return this;
    }

    public S3SelectJsonOption getS3SelectJsonOption() {
        return s3SelectJsonOption;
    }

    public ScanOptions setS3SelectJsonOption(S3SelectJsonOption s3SelectJsonOption) {
        this.s3SelectJsonOption = s3SelectJsonOption;
        return this;
    }

    public ScanOptions setStartDate(String startDate) {
        this.startDate = startDate;
        return this;
    }

    public ScanOptions setRange(String range) {
        this.range = range;
        return this;
    }

    public ScanOptions setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public ScanOptions setExpression(String expression) {
        this.expression = expression;
        return this;
    }

    public ScanOptions setSerializationFormatOption(S3SelectSerializationFormatOption serializationFormatOption) {
        this.serializationFormatOption = serializationFormatOption;
        return this;
    }

    public ScanOptions setIncludeKeyPaths(List<String> includeKeyPaths) {
        this.includeKeyPaths = includeKeyPaths;
        return this;
    }

    public ScanOptions setExcludeKeyPaths(List<String> excludeKeyPaths) {
        this.excludeKeyPaths = excludeKeyPaths;
        return this;
    }

    public ScanOptions setCompressionOption(CompressionOption compressionOption) {
        this.compressionOption = compressionOption;
        return this;
    }

    public ScanOptions setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getRange() {
        return range;
    }

    public String getBucket() {
        return bucket;
    }

    public String getExpression() {
        return expression;
    }

    public S3SelectSerializationFormatOption getSerializationFormatOption() {
        return serializationFormatOption;
    }

    public CompressionOption getCompressionOption() {
        return compressionOption;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public List<String> getIncludeKeyPaths() {
        return includeKeyPaths;
    }

    public List<String> getExcludeKeyPaths() {
        return excludeKeyPaths;
    }
}