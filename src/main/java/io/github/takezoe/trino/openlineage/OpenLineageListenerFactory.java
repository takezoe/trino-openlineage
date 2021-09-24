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

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;
import java.util.Optional;

public class OpenLineageListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "openlineage";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        System.out.println("==== config ====");
        System.out.println(config);
        String url = config.get("openlineage.url");
        String apiKey = config.get("openlineage.apikey");
        return new OpenLineageListener(url, Optional.ofNullable(apiKey));
    }
}
