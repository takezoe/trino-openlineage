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
import io.trino.spi.eventlistener.OutputColumnMetadata;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import io.trino.spi.eventlistener.QueryStatistics;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
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

    private UUID getQueryId(QueryMetadata queryMetadata)
    {
        return UUID.nameUUIDFromBytes(queryMetadata.getQueryId().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        UUID runID = getQueryId(queryCreatedEvent.getMetadata());

        try {
            sendStartEvent(runID, queryCreatedEvent);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        UUID runID = getQueryId(queryCompletedEvent.getMetadata());

        try {
            sendCompletedEvent(runID, queryCompletedEvent);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<OpenLineage.RunFacet> getTrinoMetadataFacet(QueryMetadata queryMetadata)
    {
        if (this.trinoMetadataFacetEnabled) {
            OpenLineage.RunFacet trinoMetadataFacet = ol.newRunFacet();

            if (queryMetadata.getPlan().isPresent()) {
                trinoMetadataFacet.getAdditionalProperties().put("queryPlan", queryMetadata.getPlan().orElse(""));
            }
            if (queryMetadata.getTransactionId().isPresent()) {
                trinoMetadataFacet.getAdditionalProperties().put("transactionId", queryMetadata.getTransactionId().orElse(""));
            }
            return Optional.of(trinoMetadataFacet);
        }
        return Optional.empty();
    }

    private Optional<OpenLineage.RunFacet> getTrinoQueryStatisticsFacet(QueryStatistics queryStatistics)
            throws IllegalAccessException
    {
        if (this.queryStatisticsFacetEnabled) {
            OpenLineage.RunFacet trinoQueryStatisticsFacet = ol.newRunFacet();

            for (Field field : queryStatistics.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                trinoQueryStatisticsFacet
                        .getAdditionalProperties()
                        .put(field.getName(), String.valueOf(field.get(queryStatistics)));
            }
            return Optional.of(trinoQueryStatisticsFacet);
        }
        return Optional.empty();
    }

    private void sendStartEvent(UUID runID, QueryCreatedEvent queryCreatedEvent)
            throws IllegalAccessException
    {
        OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();
        Optional<OpenLineage.RunFacet> trinoMetadata = getTrinoMetadataFacet(queryCreatedEvent.getMetadata());

        trinoMetadata.ifPresent(runFacet -> runFacetsBuilder.put("trino.metadata", runFacet));

        OpenLineage.RunEvent startEvent =
                ol.newRunEventBuilder()
                        .eventType(OpenLineage.RunEvent.EventType.START)
                        .eventTime(queryCreatedEvent.getCreateTime().atZone(ZoneId.of("UTC")))
                        .run(ol.newRunBuilder().runId(runID).facets(runFacetsBuilder.build()).build())
                        .job(
                                ol.newJobBuilder()
                                        .namespace(queryCreatedEvent.getContext().getUser())
                                        .name(queryCreatedEvent.getMetadata().getQueryId())
                                        .facets(
                                                ol.newJobFacetsBuilder()
                                                        .sql(ol.newSQLJobFacet(queryCreatedEvent.getMetadata().getQuery()))
                                                        .build())
                                        .build())
                        .build();

        client.emit(startEvent);
    }

    private void sendCompletedEvent(UUID runID, QueryCompletedEvent queryCompletedEvent)
            throws IllegalAccessException
    {
        boolean failed = queryCompletedEvent.getMetadata().getQueryState().equals("FAILED");

        OpenLineage.RunFacetsBuilder runFacetsBuilder = ol.newRunFacetsBuilder();
        Optional<OpenLineage.RunFacet> trinoMetadata = getTrinoMetadataFacet(queryCompletedEvent.getMetadata());
        Optional<OpenLineage.RunFacet> trinoQueryStatistics = getTrinoQueryStatisticsFacet(queryCompletedEvent.getStatistics());

        trinoMetadata.ifPresent(runFacet -> runFacetsBuilder.put("trino.metadata", runFacet));
        trinoQueryStatistics.ifPresent(runFacet -> runFacetsBuilder.put("trino.queryStatistics", runFacet));

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
                        .facets(ol.newDatasetFacetsBuilder()
                                .schema(ol.newSchemaDatasetFacetBuilder()
                                    .fields(
                                        inputMetadata
                                                .getColumns()
                                                .stream()
                                                .map(field -> ol.newSchemaDatasetFacetFieldsBuilder()
                                                        .name(field)
                                                        .build()
                                                ).toList())
                                    .build()
                        ).build())
                        .build()
        ).collect(toImmutableList());
    }

    private List<OpenLineage.OutputDataset> buildOutputs(QueryIOMetadata ioMetadata)
    {
        Optional<QueryOutputMetadata> outputs = ioMetadata.getOutput();
        if (outputs.isPresent()) {
            QueryOutputMetadata outputMetadata = outputs.get();
            List<OutputColumnMetadata> outputColumns = outputMetadata.getColumns().orElse(new ArrayList<>());

            OpenLineage.ColumnLineageDatasetFacetBuilder columnLineageBuilder = ol.newColumnLineageDatasetFacetBuilder();

            outputColumns.forEach(column ->
                    columnLineageBuilder.put(column.getColumnName(),
                            ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                                    .inputFields(column
                                            .getSourceColumns()
                                            .stream()
                                            .map(inputColumn -> ol.newColumnLineageDatasetFacetFieldsAdditionalInputFieldsBuilder()
                                                    .field(inputColumn.getColumnName())
                                                    .namespace(inputColumn.getCatalog())
                                                    .name(inputColumn.getSchema() + "." + inputColumn.getTable())
                                                    .build())
                                            .toList()
                                    ).build()));

            return ImmutableList.of(
                    ol.newOutputDatasetBuilder()
                            .namespace(getDatasetNamespace(outputMetadata.getCatalogName()))
                            .name(outputMetadata.getSchema() + "." + outputMetadata.getTable())
                            .facets(ol.newDatasetFacetsBuilder()
                                    .columnLineage(columnLineageBuilder.build())
                                    .schema(ol.newSchemaDatasetFacetBuilder()
                                            .fields(
                                                    outputColumns.stream()
                                                            .map(column -> ol.newSchemaDatasetFacetFieldsBuilder()
                                                                    .name(column.getColumnName())
                                                                    .type(column.getColumnType())
                                                                    .build())
                                                            .toList()
                                            ).build()
                                    ).build()
                            ).build());
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
