/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.takezoe.trino.openlineage;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class OpenLineageListener
        implements EventListener
{
    private final OpenLineage ol = new OpenLineage(URI.create("https://github.com/takezoe/trino-openlineage"));
    private final OpenLineageClient client;
    private final Boolean trinoMetadataFacetEnabled;
    private final Boolean queryStatisticsFacetEnabled;

    public OpenLineageListener(String url, Optional<String> apiKey, Boolean trinoMetadataFacetEnabled, Boolean queryStatisticsFacetEnabled)
    {
        this.client = new OpenLineageClient(url, apiKey);
        this.trinoMetadataFacetEnabled = trinoMetadataFacetEnabled;
        this.queryStatisticsFacetEnabled = queryStatisticsFacetEnabled;
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        // Do nothing here
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        UUID runID = UUID.randomUUID();
        try {
            sendStartEvent(runID, queryCompletedEvent);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        try {
            sendCompletedEvent(runID, queryCompletedEvent);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendStartEvent(UUID runID, QueryCompletedEvent queryCompletedEvent)
            throws IllegalAccessException
    {
        OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();

        if (this.trinoMetadataFacetEnabled) {
            OpenLineage.RunFacet trinoMetadataFacet = ol.newRunFacet();
            QueryMetadata trinoMetadata = queryCompletedEvent.getMetadata();

            trinoMetadataFacet.getAdditionalProperties().put("queryPlan", String.valueOf(trinoMetadata.getPlan()));
            trinoMetadataFacet.getAdditionalProperties().put("transactionId", String.valueOf(trinoMetadata.getTransactionId()));

            runFacetsBuilder.put("trino.metadata", trinoMetadataFacet);
        }

        if (this.queryStatisticsFacetEnabled) {
            OpenLineage.RunFacet trinoQueryStatisticsFacet = ol.newRunFacet();
            QueryStatistics trinoQueryStatistics = queryCompletedEvent.getStatistics();

            for (Field field : trinoQueryStatistics.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                trinoQueryStatisticsFacet
                        .getAdditionalProperties()
                        .put(field.getName(), String.valueOf(field.get(trinoQueryStatistics)));
            }

            runFacetsBuilder.put("trino.queryStatistics", trinoQueryStatisticsFacet);
        }

        OpenLineage.RunEvent startEvent =
                ol.newRunEventBuilder()
                        .eventType(OpenLineage.RunEvent.EventType.START)
                        .eventTime(queryCompletedEvent.getExecutionStartTime().atZone(ZoneId.of("UTC")))
                        .run(ol.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                        .job(
                                ol.newJobBuilder()
                                        .namespace(queryCompletedEvent.getContext().getUser())
                                        .name(queryCompletedEvent.getMetadata().getQueryId())
                                        .facets(
                                                ol.newJobFacetsBuilder()
                                                        .sql(ol.newSQLJobFacet(queryCompletedEvent.getMetadata().getQuery()))
                                                        .build())
                                        .build())
                        .inputs(buildInputs(queryCompletedEvent.getIoMetadata()))
                        .outputs(buildOutputs(queryCompletedEvent.getIoMetadata()))
                        .build();

        client.emit(startEvent);
    }

    private void sendCompletedEvent(UUID runID, QueryCompletedEvent queryCompletedEvent)
            throws IllegalAccessException
    {
        boolean failed = queryCompletedEvent.getMetadata().getQueryState().equals("FAILED");

        OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();

        if (this.trinoMetadataFacetEnabled) {
            OpenLineage.RunFacet trinoMetadataFacet = ol.newRunFacet();
            QueryMetadata trinoMetadata = queryCompletedEvent.getMetadata();

            trinoMetadataFacet.getAdditionalProperties().put("queryPlan", String.valueOf(trinoMetadata.getPlan()));
            trinoMetadataFacet.getAdditionalProperties().put("transactionId", String.valueOf(trinoMetadata.getTransactionId()));

            runFacetsBuilder.put("trino.metadata", trinoMetadataFacet);
        }

        if (this.queryStatisticsFacetEnabled) {
            OpenLineage.RunFacet trinoQueryStatisticsFacet = ol.newRunFacet();
            QueryStatistics trinoQueryStatistics = queryCompletedEvent.getStatistics();

            for (Field field : trinoQueryStatistics.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                trinoQueryStatisticsFacet
                        .getAdditionalProperties()
                        .put(field.getName(), String.valueOf(field.get(trinoQueryStatistics)));
            }

            runFacetsBuilder.put("trino.queryStatistics", trinoQueryStatisticsFacet);
        }

        OpenLineage.RunEvent completedEvent =
                ol.newRunEventBuilder()
                        .eventType(
                                failed
                                        ? OpenLineage.RunEvent.EventType.FAIL
                                        : OpenLineage.RunEvent.EventType.COMPLETE)
                        .eventTime(queryCompletedEvent.getEndTime().atZone(ZoneId.of("UTC")))
                        .run(ol.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                        .job(
                                ol.newJobBuilder()
                                        .namespace(queryCompletedEvent.getContext().getUser())
                                        .name(queryCompletedEvent.getMetadata().getQueryId())
                                        .facets(
                                                ol.newJobFacetsBuilder()
                                                        .sql(ol.newSQLJobFacet(queryCompletedEvent.getMetadata().getQuery()))
                                                        .build())
                                        .build())
                        .inputs(buildInputs(queryCompletedEvent.getIoMetadata()))
                        .outputs(buildOutputs(queryCompletedEvent.getIoMetadata()))
                        .build();

        client.emit(completedEvent);
    }

    private List<OpenLineage.InputDataset> buildInputs(QueryIOMetadata ioMetadata)
    {
        return ioMetadata.getInputs().stream().map(inputMetadata ->
                ol.newInputDatasetBuilder()
                        .namespace(getDatasetNamespace(inputMetadata.getCatalogName()))
                        .name(inputMetadata.getSchema() + "." + inputMetadata.getTable())
                        .build()
        ).collect(toImmutableList());
    }

    private List<OpenLineage.OutputDataset> buildOutputs(QueryIOMetadata ioMetadata)
    {
        Optional<QueryOutputMetadata> outputs = ioMetadata.getOutput();
        if (outputs.isPresent()) {
            QueryOutputMetadata outputMetadata = outputs.get();
            return ImmutableList.of(
                    ol.newOutputDatasetBuilder()
                            .namespace(getDatasetNamespace(outputMetadata.getCatalogName()))
                            .name(outputMetadata.getSchema() + "." + outputMetadata.getTable())
                            .build());
        }
        else {
            return ImmutableList.of();
        }
    }

    private String getDatasetNamespace(String catalogName)
    {
        int index = catalogName.indexOf('@');
        if (index >= 0) {
            return catalogName.substring(index + 1);
        }
        else {
            return catalogName;
        }
    }
}
