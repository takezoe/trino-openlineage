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

import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OpenLineageListenerFactory
        implements EventListenerFactory
{
    private static final Logger logger = Logger.get(OpenLineageListenerFactory.class);

    @Override
    public String getName()
    {
        return "openlineage";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        String url = requireNonNull(config.get("openlineage.url"));
        String apiKey = config.get("openlineage.apikey");
        Boolean trinoMetadataFacetEnabled = Optional.ofNullable(config.get("openlineage.facets.trinoMetadata.enabled")).orElse("true").equalsIgnoreCase("true");
        Boolean trinoQueryContextFacetEnabled = Optional.ofNullable(config.get("openlineage.facets.trinoQueryContext.enabled")).orElse("true").equalsIgnoreCase("true");
        Boolean trinoQueryStatisticsFacetEnabled = Optional.ofNullable(config.get("openlineage.facets.trinoQueryStatistics.enabled")).orElse("true").equalsIgnoreCase("true");
        String namespace = config.get("openlineage.namespace");

        logger.info("openlineage.url: " + url);

        return new OpenLineageListener(url, Optional.ofNullable(namespace), Optional.ofNullable(apiKey), trinoMetadataFacetEnabled, trinoQueryContextFacetEnabled, trinoQueryStatisticsFacetEnabled);
    }
}
