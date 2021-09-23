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
import io.openlineage.client.OpenLineage;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class OpenLineageClient
{
    private final JettyHttpClient jettyClient;
    private final Optional<String> apiKey;
    private static final ObjectMapper objectMapper = createMapper();

    public OpenLineageClient(Optional<String> apiKey)
    {
        this.jettyClient = new JettyHttpClient();
        this.apiKey = apiKey;
    }

    public void emit(OpenLineage.RunEvent event)
    {
        try {
            System.out.println(objectMapper.writeValueAsString(event)); // TODO logging

            Request.Builder requestBuilder = Request.builder()
                    .setMethod("POST")
                    .setUri(URI.create("http://localhost:5000/api/v1/lineage")) // TODO configurable
                    .addHeader("Content-Type", "application/json")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8)));

            if (apiKey.isPresent()) {
                requestBuilder.addHeader("Authorization", "Bearer " + apiKey.get());
            }

            StatusResponseHandler.StatusResponse status = jettyClient.execute(requestBuilder.build(), StatusResponseHandler.createStatusResponseHandler());
            System.out.println(status.getStatusCode());
        }
        catch (Exception e) {
            e.printStackTrace(); // TODO logging
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
