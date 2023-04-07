/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.parser.model.PipelineConfiguration;
import org.opensearch.dataprepper.parser.model.RoutedPluginSetting;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.peerforwarder.PeerForwardingProcessorDecorator;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.PipelineConnector;
import org.opensearch.dataprepper.pipeline.router.Router;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.plugins.MultiBufferDecorator;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

@SuppressWarnings("rawtypes")
public class PipelineParser {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    private static final String PIPELINE_TYPE = "pipeline";
    private static final String ATTRIBUTE_NAME = "name";
    private final String pipelineConfigurationFileLocation;
    private final RouterFactory routerFactory;
    private final DataPrepperConfiguration dataPrepperConfiguration;
    private final CircuitBreakerManager circuitBreakerManager;
    private final Map<String, PipelineConnector> sourceConnectorMap = new HashMap<>(); //TODO Remove this and rely only on pipelineMap
    private final PluginFactory pluginFactory;
    private final PeerForwarderProvider peerForwarderProvider;
    private final EventFactory eventFactory;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final SourceCoordinatorFactory sourceCoordinatorFactory;

    public PipelineParser(final String pipelineConfigurationFileLocation,
                          final PluginFactory pluginFactory,
                          final PeerForwarderProvider peerForwarderProvider,
                          final RouterFactory routerFactory,
                          final DataPrepperConfiguration dataPrepperConfiguration,
                          final CircuitBreakerManager circuitBreakerManager,
                          final EventFactory eventFactory,
                          final AcknowledgementSetManager acknowledgementSetManager,
                          final SourceCoordinatorFactory sourceCoordinatorFactory) {
        this.pipelineConfigurationFileLocation = pipelineConfigurationFileLocation;
        this.pluginFactory = Objects.requireNonNull(pluginFactory);
        this.peerForwarderProvider = Objects.requireNonNull(peerForwarderProvider);
        this.routerFactory = routerFactory;
        this.dataPrepperConfiguration = Objects.requireNonNull(dataPrepperConfiguration);
        this.circuitBreakerManager = circuitBreakerManager;
        this.eventFactory = eventFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceCoordinatorFactory = sourceCoordinatorFactory;
    }

    /**
     * Parses the configuration file into Pipeline
     */
    public Map<String, Pipeline> parseConfiguration() {
        try (final InputStream mergedPipelineConfigurationFiles = mergePipelineConfigurationFiles()) {
            final PipelinesDataFlowModel pipelinesDataFlowModel = OBJECT_MAPPER.readValue(mergedPipelineConfigurationFiles,
                    PipelinesDataFlowModel.class);

            final DataPrepperVersion version = pipelinesDataFlowModel.getDataPrepperVersion();
            validateDataPrepperVersion(version);

            final Map<String, PipelineConfiguration> pipelineConfigurationMap = pipelinesDataFlowModel.getPipelines().entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new PipelineConfiguration(entry.getValue())
                    ));
            final List<String> allPipelineNames = PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigurationMap);

            // LinkedHashMap to preserve insertion order
            final Map<String, Pipeline> pipelineMap = new LinkedHashMap<>();
            pipelineConfigurationMap.forEach((pipelineName, configuration) ->
                    configuration.updateCommonPipelineConfiguration(pipelineName));
            for (String pipelineName : allPipelineNames) {
                if (!pipelineMap.containsKey(pipelineName) && pipelineConfigurationMap.containsKey(pipelineName)) {
                    buildPipelineFromConfiguration(pipelineName, pipelineConfigurationMap, pipelineMap);
                }
            }
            return pipelineMap;
        } catch (IOException e) {
            LOG.error("Failed to parse the configuration file {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Failed to parse the configuration file %s", pipelineConfigurationFileLocation), e);
        }
    }

    private void validateDataPrepperVersion(final DataPrepperVersion version) {
        if (Objects.nonNull(version) && !DataPrepperVersion.getCurrentVersion().compatibleWith(version)) {
            LOG.error("The version: {} is not compatible with the current version: {}", version, DataPrepperVersion.getCurrentVersion());
            throw new ParseException(format("The version: %s is not compatible with the current version: %s",
                version, DataPrepperVersion.getCurrentVersion()));
        }
    }

    private InputStream mergePipelineConfigurationFiles() throws IOException {
        final File configurationLocation = new File(pipelineConfigurationFileLocation);

        if (configurationLocation.isFile()) {
            return new FileInputStream(configurationLocation);
        } else if (configurationLocation.isDirectory()) {
            FileFilter yamlFilter = pathname -> (pathname.getName().endsWith(".yaml") || pathname.getName().endsWith(".yml"));
            List<InputStream> configurationFiles = Stream.of(configurationLocation.listFiles(yamlFilter))
                    .map(file -> {
                        InputStream inputStream;
                        try {
                            inputStream = new FileInputStream(file);
                            LOG.info("Reading pipeline configuration from {}", file.getName());
                        } catch (FileNotFoundException e) {
                            inputStream = null;
                            LOG.warn("Pipeline configuration file {} not found", file.getName());
                        }
                        return inputStream;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (configurationFiles.isEmpty()) {
                LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
                throw new ParseException(
                        format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
            }

            return new SequenceInputStream(Collections.enumeration(configurationFiles));
        } else {
            LOG.error("Pipelines configuration file not found at {}", pipelineConfigurationFileLocation);
            throw new ParseException(format("Pipelines configuration file not found at %s", pipelineConfigurationFileLocation));
        }
    }

    private void buildPipelineFromConfiguration(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipelineName);
        LOG.info("Building pipeline [{}] from provided configuration", pipelineName);
        try {
            final PluginSetting sourceSetting = pipelineConfiguration.getSourcePluginSetting();
            final Optional<Source> pipelineSource = getSourceIfPipelineType(pipelineName, sourceSetting,
                    pipelineMap, pipelineConfigurationMap);
            final Source source = pipelineSource.orElseGet(() ->
                    pluginFactory.loadPlugin(Source.class, sourceSetting));



            LOG.info("Building buffer for the pipeline [{}]", pipelineName);
            final Buffer pipelineDefinedBuffer = pluginFactory.loadPlugin(Buffer.class, pipelineConfiguration.getBufferPluginSetting());

            LOG.info("Building processors for the pipeline [{}]", pipelineName);
            final int processorThreads = pipelineConfiguration.getWorkers();

            final List<List<IdentifiedComponent<Processor>>> processorSets = pipelineConfiguration.getProcessorPluginSettings().stream()
                    .map(this::newProcessor)
                    .collect(Collectors.toList());

            final List<List<Processor>> decoratedProcessorSets = processorSets.stream()
                    .map(processorComponentList -> {
                        final List<Processor> processors = processorComponentList.stream().map(IdentifiedComponent::getComponent).collect(Collectors.toList());
                        if (!processors.isEmpty() && processors.get(0) instanceof RequiresPeerForwarding) {
                            return PeerForwardingProcessorDecorator.decorateProcessors(
                                    processors, peerForwarderProvider, pipelineName, processorComponentList.get(0).getName(), pipelineConfiguration.getWorkers()
                            );
                        }
                        return processors;
                    }).collect(Collectors.toList());

            final int readBatchDelay = pipelineConfiguration.getReadBatchDelay();

            LOG.info("Building sinks for the pipeline [{}]", pipelineName);
            final List<DataFlowComponent<Sink>> sinks = pipelineConfiguration.getSinkPluginSettings().stream()
                    .map(this::buildRoutedSinkOrConnector)
                    .collect(Collectors.toList());

            final List<Buffer> secondaryBuffers = getSecondaryBuffers();
            LOG.info("Constructing MultiBufferDecorator with [{}] secondary buffers for pipeline [{}]", secondaryBuffers.size(), pipelineName);
            final MultiBufferDecorator multiBufferDecorator = new MultiBufferDecorator(pipelineDefinedBuffer, secondaryBuffers);


            final Buffer buffer;
            if(source instanceof PipelineConnector) {
                buffer = multiBufferDecorator;
            } else {
                buffer = circuitBreakerManager.getGlobalCircuitBreaker()
                        .map(circuitBreaker -> new CircuitBreakingBuffer<>(multiBufferDecorator, circuitBreaker))
                        .map(b -> (Buffer)b)
                        .orElseGet(() -> multiBufferDecorator);
            }

            final Router router = routerFactory.createRouter(pipelineConfiguration.getRoutes());

            final Pipeline pipeline = new Pipeline(pipelineName, source, buffer, decoratedProcessorSets, sinks, router,
                    eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, processorThreads, readBatchDelay,
                    dataPrepperConfiguration.getProcessorShutdownTimeout(), dataPrepperConfiguration.getSinkShutdownTimeout(),
                    getPeerForwarderDrainTimeout(dataPrepperConfiguration));
            pipelineMap.put(pipelineName, pipeline);
        } catch (Exception ex) {
            //If pipeline construction errors out, we will skip that pipeline and proceed
            LOG.error("Construction of pipeline components failed, skipping building of pipeline [{}] and its connected " +
                    "pipelines", pipelineName, ex);
            processRemoveIfRequired(pipelineName, pipelineConfigurationMap, pipelineMap);
        }

    }

    private List<IdentifiedComponent<Processor>> newProcessor(final PluginSetting pluginSetting) {
        final List<Processor> processors = pluginFactory.loadPlugins(
                Processor.class,
                pluginSetting,
                actualClass -> actualClass.isAnnotationPresent(SingleThread.class) ?
                        pluginSetting.getNumberOfProcessWorkers() :
                        1);

        return processors.stream()
                .map(processor -> new IdentifiedComponent<>(processor, pluginSetting.getName()))
                .collect(Collectors.toList());
    }

    private Optional<Source> getSourceIfPipelineType(
            final String sourcePipelineName,
            final PluginSetting pluginSetting,
            final Map<String, Pipeline> pipelineMap,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        LOG.info("Building [{}] as source component for the pipeline [{}]", pluginSetting.getName(), sourcePipelineName);
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String connectedPipeline = pipelineNameOptional.get();
            if (!sourceConnectorMap.containsKey(sourcePipelineName)) {
                LOG.info("Source of pipeline [{}] requires building of pipeline [{}]", sourcePipelineName,
                        connectedPipeline);
                //Build connected pipeline for the pipeline connector to be available
                //Building like below sometimes yields multiple runs if the pipeline building fails before sink
                //creation. except for running the creation again, it will not harm anything - TODO Fix this
                buildPipelineFromConfiguration(pipelineNameOptional.get(), pipelineConfigurationMap, pipelineMap);
            }
            if (!pipelineMap.containsKey(connectedPipeline)) {
                LOG.error("Connected Pipeline [{}] failed to build, Failing building source for [{}]",
                        connectedPipeline, sourcePipelineName);
                throw new RuntimeException(format("Failed building source for %s, exiting", sourcePipelineName));
            }
            Pipeline sourcePipeline = pipelineMap.get(connectedPipeline);
            final PipelineConnector pipelineConnector = sourceConnectorMap.get(sourcePipelineName);
            pipelineConnector.setSourcePipelineName(pipelineNameOptional.get());
            if (sourcePipeline.getSource().areAcknowledgementsEnabled()) {
                pipelineConnector.enableAcknowledgements();
            }
            return Optional.of(pipelineConnector);
        }
        return Optional.empty();
    }

    private DataFlowComponent<Sink> buildRoutedSinkOrConnector(final RoutedPluginSetting pluginSetting) {
        final Sink sink = buildSinkOrConnector(pluginSetting);

        return new DataFlowComponent<>(sink, pluginSetting.getRoutes());
    }

    private Sink buildSinkOrConnector(final PluginSetting pluginSetting) {
        LOG.info("Building [{}] as sink component", pluginSetting.getName());
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String pipelineName = pipelineNameOptional.get();
            final PipelineConnector pipelineConnector = new PipelineConnector(pipelineName);
            sourceConnectorMap.put(pipelineName, pipelineConnector); //TODO retrieve from parent Pipeline using name
            return pipelineConnector;
        } else {
            return pluginFactory.loadPlugin(Sink.class, pluginSetting);
        }
    }

    private Optional<String> getPipelineNameIfPipelineType(final PluginSetting pluginSetting) {
        if (PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME) != null) {
            //Validator marked valid config with type as pipeline will have attribute name
            return Optional.of((String) pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME));
        }
        return Optional.empty();
    }

    /**
     * This removes all built connected pipelines of given pipeline from pipelineMap.
     * TODO Update this to be more elegant and trigger destroy of plugins
     */
    private void removeConnectedPipelines(
            final String failedPipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration failedPipelineConfiguration = pipelineConfigurationMap.remove(failedPipeline);

        //remove source connected pipelines
        final Optional<String> sourcePipelineOptional = getPipelineNameIfPipelineType(
                failedPipelineConfiguration.getSourcePluginSetting());
        sourcePipelineOptional.ifPresent(sourcePipeline -> processRemoveIfRequired(
                sourcePipeline, pipelineConfigurationMap, pipelineMap));

        //remove sink connected pipelines
        final List<RoutedPluginSetting> sinkPluginSettings = failedPipelineConfiguration.getSinkPluginSettings();
        sinkPluginSettings.forEach(sinkPluginSetting -> {
            getPipelineNameIfPipelineType(sinkPluginSetting).ifPresent(sinkPipeline -> processRemoveIfRequired(
                    sinkPipeline, pipelineConfigurationMap, pipelineMap));
        });
    }

    private void processRemoveIfRequired(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        if (pipelineConfigurationMap.containsKey(pipelineName)) {
            pipelineMap.remove(pipelineName);
            sourceConnectorMap.remove(pipelineName);
            removeConnectedPipelines(pipelineName, pipelineConfigurationMap, pipelineMap);
        }
    }

    private static class IdentifiedComponent<T> {
        private final T component;
        private final String name;

        private IdentifiedComponent(final T component, final String name) {
            this.component = component;
            this.name = name;
        }

        T getComponent() {
            return component;
        }

        String getName() {
            return name;
        }
    }

    Duration getPeerForwarderDrainTimeout(final DataPrepperConfiguration dataPrepperConfiguration) {
        return Optional.ofNullable(dataPrepperConfiguration)
                .map(DataPrepperConfiguration::getPeerForwarderConfiguration)
                .map(PeerForwarderConfiguration::getDrainTimeout)
                .orElse(Duration.ofSeconds(0));
    }

    List<Buffer> getSecondaryBuffers() {
        return peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap().entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream())
                .map(innerEntry -> innerEntry.getValue())
                .collect(Collectors.toList());
    }
}
