/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TopicConfigTest {

    @Mock
    TopicConfig topicConfig;

    private static final String YAML_FILE_WITH_CONSUMER_CONFIG = "sample-pipelines.yaml";

    private static final String YAML_FILE_WITH_MISSING_CONSUMER_CONFIG = "sample-pipelines-1.yaml";

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        String fileName = testInfo.getTags().stream().findFirst().orElse("");
        topicConfig = new TopicConfig();
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource(fileName).getFile());
        Object data = yaml.load(fileReader);
        ObjectMapper mapper = new ObjectMapper();
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            KafkaSourceConfig kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            List<TopicConfig> topicConfigList = kafkaSourceConfig.getTopics();
            topicConfig = topicConfigList.get(0);
        }
    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void test_topicsConfig_not_null() {
        assertThat(topicConfig, notNullValue());
    }

    @Test
    @Tag(YAML_FILE_WITH_MISSING_CONSUMER_CONFIG)
    void testConfigValues_default() {
        assertEquals("my-topic-2", topicConfig.getName());
        assertEquals("DPKafkaProj-2", topicConfig.getGroupId());
        assertEquals("kafka-consumer-group-2", topicConfig.getGroupName());
        assertEquals("false", topicConfig.getAutoCommit());
        assertEquals(Duration.ofSeconds(5), topicConfig.getAutoCommitInterval());
        assertEquals(Duration.ofSeconds(45), topicConfig.getSessionTimeOut());
        assertEquals("earliest", topicConfig.getAutoOffsetReset());
        assertEquals(Duration.ofSeconds(1), topicConfig.getThreadWaitingTime());
        assertEquals(Duration.ofSeconds(4), topicConfig.getMaxRecordFetchTime());
        assertEquals(Duration.ofSeconds(5), topicConfig.getBufferDefaultTimeout());
        assertEquals(52428800L, topicConfig.getFetchMaxBytes().longValue());
        assertEquals(500L, topicConfig.getFetchMaxWait().longValue());
        assertEquals(1L, topicConfig.getFetchMinBytes().longValue());
        assertEquals(Duration.ofSeconds(100), topicConfig.getRetryBackoff());
        assertEquals(Duration.ofSeconds(300000), topicConfig.getMaxPollInterval());
        assertEquals(500L, topicConfig.getConsumerMaxPollRecords().longValue());
        assertEquals(10, topicConfig.getWorkers().intValue());
        assertEquals(Duration.ofSeconds(3), topicConfig.getHeartBeatInterval());
    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void testConfigValues_from_yaml() {

        assertEquals("my-topic-1", topicConfig.getName());
        assertEquals("DPKafkaProj-2", topicConfig.getGroupId());
        assertEquals("kafka-consumer-group-2", topicConfig.getGroupName());
        assertEquals("false", topicConfig.getAutoCommit());
        assertEquals(Duration.ofSeconds(5), topicConfig.getAutoCommitInterval());
        assertEquals(Duration.ofSeconds(45), topicConfig.getSessionTimeOut());
        assertEquals("earliest", topicConfig.getAutoOffsetReset());
        assertEquals(Duration.ofSeconds(1), topicConfig.getThreadWaitingTime());
        assertEquals(Duration.ofSeconds(4), topicConfig.getMaxRecordFetchTime());
        assertEquals(Duration.ofSeconds(5), topicConfig.getBufferDefaultTimeout());
        assertEquals(52428800L, topicConfig.getFetchMaxBytes().longValue());
        assertEquals(500L, topicConfig.getFetchMaxWait().longValue());
        assertEquals(1L, topicConfig.getFetchMinBytes().longValue());
        assertEquals(Duration.ofSeconds(100), topicConfig.getRetryBackoff());
        assertEquals(Duration.ofSeconds(300000), topicConfig.getMaxPollInterval());
        assertEquals(500L, topicConfig.getConsumerMaxPollRecords().longValue());
        assertEquals(10, topicConfig.getWorkers().intValue());
        assertEquals(Duration.ofSeconds(3), topicConfig.getHeartBeatInterval());
    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void testConfigValues_from_yaml_not_null() {

        assertNotNull(topicConfig.getName());
        assertNotNull(topicConfig.getGroupId());
        assertNotNull(topicConfig.getGroupName());
        assertNotNull(topicConfig.getAutoCommit());
        assertNotNull(topicConfig.getAutoCommitInterval());
        assertNotNull(topicConfig.getSessionTimeOut());
        assertNotNull(topicConfig.getAutoOffsetReset());
        assertNotNull(topicConfig.getThreadWaitingTime());
        assertNotNull(topicConfig.getMaxRecordFetchTime());
        assertNotNull(topicConfig.getBufferDefaultTimeout());
        assertNotNull(topicConfig.getFetchMaxBytes());
        assertNotNull(topicConfig.getFetchMaxWait());
        assertNotNull(topicConfig.getFetchMinBytes());
        assertNotNull(topicConfig.getRetryBackoff());
        assertNotNull(topicConfig.getMaxPollInterval());
        assertNotNull(topicConfig.getConsumerMaxPollRecords());
        assertNotNull(topicConfig.getWorkers());
        assertNotNull(topicConfig.getHeartBeatInterval());
    }

}
