/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchWithSearchAfterResults;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.PitWorker.EXTEND_KEEP_ALIVE_TIME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.PitWorker.STARTING_KEEP_ALIVE;

@ExtendWith(MockitoExtension.class)
public class PitWorkerTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    @Mock
    private SearchAccessor searchAccessor;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        executorService = Executors.newSingleThreadExecutor();
    }

    private PitWorker createObjectUnderTest() {
        return new PitWorker(searchAccessor, openSearchSourceConfiguration, sourceCoordinator, bufferAccumulator, openSearchIndexPartitionCreationSupplier);
    }

    @Test
    void run_with_getNextPartition_returning_empty_will_sleep_and_exit_when_interrupted() throws InterruptedException {
        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.empty());


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));
    }

    @Test
    void run_with_getNextPartition_with_non_empty_partition_creates_and_deletes_pit_and_closes_that_partition() throws Exception {
        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);
        when(sourcePartition.getPartitionState()).thenReturn(Optional.empty());

        final String pitId = UUID.randomUUID().toString();
        final ArgumentCaptor<CreatePointInTimeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(CreatePointInTimeRequest.class);
        final CreatePointInTimeResponse createPointInTimeResponse = mock(CreatePointInTimeResponse.class);
        when(createPointInTimeResponse.getPitId()).thenReturn(pitId);
        when(searchAccessor.createPit(requestArgumentCaptor.capture())).thenReturn(createPointInTimeResponse);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchWithSearchAfterResults searchWithSearchAfterResults = mock(SearchWithSearchAfterResults.class);
        when(searchWithSearchAfterResults.getNextSearchAfter()).thenReturn(Collections.singletonList(UUID.randomUUID().toString()));
        when(searchWithSearchAfterResults.getDocuments()).thenReturn(List.of(mock(Event.class), mock(Event.class))).thenReturn(List.of(mock(Event.class), mock(Event.class)))
                .thenReturn(List.of(mock(Event.class))).thenReturn(List.of(mock(Event.class)));

        final ArgumentCaptor<SearchPointInTimeRequest> searchPointInTimeRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPointInTimeRequest.class);
        when(searchAccessor.searchWithPit(searchPointInTimeRequestArgumentCaptor.capture())).thenReturn(searchWithSearchAfterResults);

        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();

        final ArgumentCaptor<DeletePointInTimeRequest> deleteRequestArgumentCaptor = ArgumentCaptor.forClass(DeletePointInTimeRequest.class);
        doNothing().when(searchAccessor).deletePit(deleteRequestArgumentCaptor.capture());

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition)).thenReturn(Optional.empty());

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getJobCount()).thenReturn(1);
        when(schedulingParameterConfiguration.getRate()).thenReturn(Duration.ZERO);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        doNothing().when(sourceCoordinator).closePartition(partitionKey,
                Duration.ZERO, 1);


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));

        final CreatePointInTimeRequest createPointInTimeRequest = requestArgumentCaptor.getValue();
        assertThat(createPointInTimeRequest, notNullValue());
        assertThat(createPointInTimeRequest.getIndex(), equalTo(partitionKey));
        assertThat(createPointInTimeRequest.getKeepAlive(), equalTo(STARTING_KEEP_ALIVE));

        verify(searchAccessor, times(2)).searchWithPit(any(SearchPointInTimeRequest.class));
        verify(sourceCoordinator, times(2)).saveProgressStateForPartition(eq(partitionKey), any(OpenSearchIndexProgressState.class));

        final List<SearchPointInTimeRequest> searchPointInTimeRequestList = searchPointInTimeRequestArgumentCaptor.getAllValues();
        assertThat(searchPointInTimeRequestList.size(), equalTo(2));
        assertThat(searchPointInTimeRequestList.get(0), notNullValue());
        assertThat(searchPointInTimeRequestList.get(0).getPitId(), equalTo(pitId));
        assertThat(searchPointInTimeRequestList.get(0).getKeepAlive(), equalTo(EXTEND_KEEP_ALIVE_TIME));
        assertThat(searchPointInTimeRequestList.get(0).getPaginationSize(), equalTo(2));
        assertThat(searchPointInTimeRequestList.get(0).getSearchAfter(), equalTo(null));

        assertThat(searchPointInTimeRequestList.get(1), notNullValue());
        assertThat(searchPointInTimeRequestList.get(1).getPitId(), equalTo(pitId));
        assertThat(searchPointInTimeRequestList.get(1).getKeepAlive(), equalTo(EXTEND_KEEP_ALIVE_TIME));
        assertThat(searchPointInTimeRequestList.get(1).getPaginationSize(), equalTo(2));
        assertThat(searchPointInTimeRequestList.get(1).getSearchAfter(), equalTo(searchWithSearchAfterResults.getNextSearchAfter()));


        final DeletePointInTimeRequest deletePointInTimeRequest = deleteRequestArgumentCaptor.getValue();
        assertThat(deletePointInTimeRequest, notNullValue());
        assertThat(deletePointInTimeRequest.getPitId(), equalTo(pitId));
    }

    @Test
    void run_with_getNextPartition_with_valid_existing_point_in_time_does_not_create_another_point_in_time() throws Exception {
        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        final OpenSearchIndexProgressState openSearchIndexProgressState = mock(OpenSearchIndexProgressState.class);
        final String pitId = UUID.randomUUID().toString();
        final List<String> searchAfter = List.of(UUID.randomUUID().toString());
        when(openSearchIndexProgressState.getPitId()).thenReturn(pitId);
        when(openSearchIndexProgressState.getSearchAfter()).thenReturn(searchAfter);
        when(openSearchIndexProgressState.hasValidPointInTime()).thenReturn(true);
        when(sourcePartition.getPartitionState()).thenReturn(Optional.of(openSearchIndexProgressState));

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchWithSearchAfterResults searchWithSearchAfterResults = mock(SearchWithSearchAfterResults.class);
        when(searchWithSearchAfterResults.getNextSearchAfter()).thenReturn(Collections.singletonList(UUID.randomUUID().toString()));
        when(searchWithSearchAfterResults.getDocuments()).thenReturn(List.of(mock(Event.class), mock(Event.class))).thenReturn(List.of(mock(Event.class), mock(Event.class)))
                .thenReturn(List.of(mock(Event.class))).thenReturn(List.of(mock(Event.class)));

        when(searchAccessor.searchWithPit(any(SearchPointInTimeRequest.class))).thenReturn(searchWithSearchAfterResults);

        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();

        final ArgumentCaptor<DeletePointInTimeRequest> deleteRequestArgumentCaptor = ArgumentCaptor.forClass(DeletePointInTimeRequest.class);
        doNothing().when(searchAccessor).deletePit(deleteRequestArgumentCaptor.capture());

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition)).thenReturn(Optional.empty());

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getJobCount()).thenReturn(1);
        when(schedulingParameterConfiguration.getRate()).thenReturn(Duration.ZERO);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        doNothing().when(sourceCoordinator).closePartition(partitionKey,
                Duration.ZERO, 1);


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));

        final DeletePointInTimeRequest deletePointInTimeRequest = deleteRequestArgumentCaptor.getValue();
        assertThat(deletePointInTimeRequest, notNullValue());
        assertThat(deletePointInTimeRequest.getPitId(), equalTo(pitId));

        verify(searchAccessor, never()).createPit(any(CreatePointInTimeRequest.class));
        verify(searchAccessor, times(2)).searchWithPit(any(SearchPointInTimeRequest.class));
        verify(sourceCoordinator, times(2)).saveProgressStateForPartition(eq(partitionKey), eq(openSearchIndexProgressState));
    }

    @Test
    void run_gives_up_partitions_and_waits_when_createPit_throws_SearchContextLimitException() throws InterruptedException {
        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);
        when(sourcePartition.getPartitionState()).thenReturn(Optional.empty());

        when(searchAccessor.createPit(any(CreatePointInTimeRequest.class))).thenThrow(SearchContextLimitException.class);

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition));


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));

        verify(searchAccessor, never()).deletePit(any(DeletePointInTimeRequest.class));
        verify(sourceCoordinator).giveUpPartitions();
        verify(sourceCoordinator, never()).closePartition(anyString(), any(Duration.class), anyInt());
    }
}
