/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SNSSinkTest {

    public static final int MAX_EVENTS = 100;
    public static final int MAX_RETRIES = 5;
    public static final String BUCKET_NAME = "dataprepper";
    public static final String S3_REGION = "us-east-1";
    public static final String MAXIMUM_SIZE = "1kb";
    public static final String OBJECT_KEY_NAME_PATTERN = "my-elb-%{yyyy-MM-dd'T'hh-mm-ss}";
    public static final String CODEC_PLUGIN_NAME = "json";
    public static final String SINK_PLUGIN_NAME = "sns";
    public static final String SINK_PIPELINE_NAME = "sns-sink-pipeline";
    private SNSSinkConfig snsSinkConfig;
    private SNSSink snsSink;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private SinkContext sinkContext;

    @BeforeEach
    void setUp() {

        snsSinkConfig = mock(SNSSinkConfig.class);
        sinkContext = mock(SinkContext.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        pluginSetting = mock(PluginSetting.class);
        PluginModel pluginModel = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        when(snsSinkConfig.getBufferType()).thenReturn(BufferTypeOptions.IN_MEMORY);
        when(snsSinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(snsSinkConfig.getThresholdOptions().getEventCount()).thenReturn(MAX_EVENTS);
        when(snsSinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse(MAXIMUM_SIZE));
        when(snsSinkConfig.getThresholdOptions().getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(MAX_RETRIES));
        when(snsSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(S3_REGION));
        when(snsSinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(CODEC_PLUGIN_NAME);
        when(pluginSetting.getName()).thenReturn(SINK_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(SINK_PIPELINE_NAME);
    }

    private SNSSink createObjectUnderTest() {
        return new SNSSink(pluginSetting, snsSinkConfig, pluginFactory, sinkContext, awsCredentialsSupplier);
    }

    @Test
    void test_sns_sink_plugin_isReady_positive() {
        snsSink = createObjectUnderTest();
        Assertions.assertNotNull(snsSink);
        snsSink.doInitialize();
        assertTrue(snsSink.isReady(), "sns sink is not initialized and not ready to work");
    }

    @Test
    void test_sns_sink_plugin_isReady_negative() {
        snsSink = createObjectUnderTest();
        Assertions.assertNotNull(snsSink);
        assertFalse(snsSink.isReady(), "sns sink is initialized and ready to work");
    }
}
