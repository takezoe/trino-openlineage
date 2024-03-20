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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.openlineage.client.OpenLineage;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class OpenLineageClient
{
    private final JettyHttpClient jettyClient;
    private final String url;
    private final Optional<String> apiKey;
    private static final ObjectMapper objectMapper = createMapper();
    private static final Logger logger = Logger.get(OpenLineageClient.class);

    public OpenLineageClient(String url, Optional<String> apiKey)
    {
        this.jettyClient = new JettyHttpClient();
        this.url = url;
        this.apiKey = apiKey;
    }

    public void emit(OpenLineage.RunEvent event)
    {
        try {
            // TODO: Do asynchronously with retry if failed
            String json = objectMapper.writeValueAsString(event);
            logger.info(json);

            Request.Builder requestBuilder = Request.builder()
                    .setMethod("POST")
                    .setUri(URI.create(url + "/api/v1/lineage"))
                    .addHeader("Content-Type", "application/json")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(json.getBytes(StandardCharsets.UTF_8)));

            apiKey.ifPresent(s -> requestBuilder.addHeader("Authorization", "Bearer " + s));

            StatusResponseHandler.StatusResponse status = jettyClient.execute(requestBuilder.build(), StatusResponseHandler.createStatusResponseHandler());
            logger.info("Response status: " + status.getStatusCode());
        }
        catch (Exception e) {
            logger.error(e);
        }
    }

    public void close()
    {
        jettyClient.close();
    }

    private static ObjectMapper createMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
        return mapper;
    }
}
