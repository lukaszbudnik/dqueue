/**
 * Copyright (C) 2015-2016 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue.jaxrs.service;

import com.codahale.metrics.annotation.Timed;
import com.github.lukaszbudnik.dqueue.Item;
import com.github.lukaszbudnik.dqueue.OrderedItem;
import com.github.lukaszbudnik.dqueue.OrderedQueueClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/dqueue/v1")
public class QueueService {

    public static final String X_DQUEUE_START_TIME_HEADER = "X-Dqueue-Start-Time";
    public static final String X_DQUEUE_DEPENDENCY_HEADER = "X-Dqueue-Dependency";
    public static final String X_DQUEUE_FILTERS = "X-Dqueue-Filters";
    private final OrderedQueueClient queueClient;

    public QueueService(OrderedQueueClient queueClient) {
        this.queueClient = queueClient;
    }

    @POST
    @Path("/publish")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response publish(@FormDataParam("contents") InputStream contentsInputStream,
                            @FormDataParam("contents") FormDataContentDisposition contentsMetaData,
                            @FormDataParam("startTime") UUID startTime,
                            @HeaderParam(X_DQUEUE_FILTERS) String filters) throws Exception {
        Map<String, String> filtersMap = buildFilters(filters);
        return publishItem(contentsInputStream, startTime, null, filtersMap);
    }

    @GET
    @Path("/consume")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consume(@HeaderParam(X_DQUEUE_FILTERS) String filters) throws Exception {
        Map<String, String> filtersMap = buildFilters(filters);
        return consumeItem(filtersMap, false);
    }

    @POST
    @Path("/ordered/publish")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response publishOrdered(@FormDataParam("contents") InputStream contentsInputStream,
                                   @FormDataParam("contents") FormDataContentDisposition contentsMetaData,
                                   @FormDataParam("startTime") UUID startTime,
                                   @FormDataParam("dependency") UUID dependency,
                                   @HeaderParam(X_DQUEUE_FILTERS) String filters) throws Exception {
        Map<String, String> filtersMap = buildFilters(filters);
        return publishItem(contentsInputStream, startTime, dependency, filtersMap);
    }

    @GET
    @Path("/ordered/consume")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consumeOrdered(@HeaderParam(X_DQUEUE_FILTERS) String filterHeader) throws Exception {
        Map<String, String> filters = buildFilters(filterHeader);
        return consumeItem(filters, true);
    }

    @DELETE
    @Path("/ordered/delete/{startTime}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteOrdered(@PathParam("startTime") UUID startTime,
                                  @HeaderParam(X_DQUEUE_FILTERS) String filters) throws Exception {
        Map<String, String> filtersMap = buildFilters(filters);
        queueClient.deleteOrdered(startTime, filtersMap);
        return Response.status(Response.Status.NO_CONTENT).build();
    }

    private Response consumeItem(Map<String, String> filtersMap, boolean ordered) throws InterruptedException, java.util.concurrent.ExecutionException {
        Optional<Response> response = null;
        if (ordered) {
            Future<Optional<OrderedItem>> itemFuture = queueClient.consumeOrdered(filtersMap);
            response = createConsumeOrderedResponse(itemFuture);
        } else {
            Future<Optional<Item>> itemFuture = queueClient.consume(filtersMap);
            response = createConsumeResponse(itemFuture);
        }

        return response.orElseGet(() -> Response.noContent().build());
    }

    private Optional<Response> createConsumeResponse(Future<Optional<Item>> itemFuture) throws InterruptedException, java.util.concurrent.ExecutionException {
        return itemFuture.get().map(i -> {
            StreamingOutput streamingOutput = createStreamingOutput(i);
            return Response
                    .ok(streamingOutput)
                    .header(X_DQUEUE_START_TIME_HEADER, i.getStartTime().toString())
                    .header(X_DQUEUE_FILTERS, Joiner.on(',').withKeyValueSeparator("=").join(i.getFilters()))
                    .cacheControl(CacheControl.valueOf("no-cache"))
                    .build();
        });
    }

    private Optional<Response> createConsumeOrderedResponse(Future<Optional<OrderedItem>> itemFuture) throws InterruptedException, java.util.concurrent.ExecutionException {
        return itemFuture.get().map(i -> {
            StreamingOutput streamingOutput = createStreamingOutput(i);
            return Response
                    .ok(streamingOutput)
                    .header(X_DQUEUE_START_TIME_HEADER, i.getStartTime().toString())
                    .header(X_DQUEUE_DEPENDENCY_HEADER, i.getDependency().toString())
                    .header(X_DQUEUE_FILTERS, Joiner.on(',').withKeyValueSeparator("=").join(i.getFilters()))
                    .cacheControl(CacheControl.valueOf("no-cache"))
                    .build();
        });
    }

    private StreamingOutput createStreamingOutput(Item item) {
        return output -> {
            try {
                InputStream input = new ByteArrayInputStream(item.getContents().array());
                IOUtils.copy(input, output);
            } catch (Exception e) {
                throw new WebApplicationException(e);
            }
        };
    }

    private Map<String, String> buildFilters(String filters) {
        if (StringUtils.isBlank(filters)) {
            return ImmutableMap.of();
        }

        String[] allFilters = StringUtils.split(filters, ",");
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String filter: allFilters) {
            String[] kv = StringUtils.split(filter, "=");
            if (kv.length != 2 || StringUtils.isBlank(kv[1])) {
                throw new IllegalArgumentException("Filter path segments must be paired 'key=value'");
            }
            builder.put(kv[0], kv[1]);
        }

        return builder.build();
    }

    private Response publishItem(InputStream contentsInputStream, UUID startTime, UUID dependency, Map<String, String> filtersMap) throws IOException, ExecutionException, InterruptedException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(contentsInputStream));
        if (dependency != null) {
            OrderedItem item = new OrderedItem(startTime, dependency, byteBuffer, filtersMap);
            queueClient.publishOrdered(item).get();
        } else {
            Item item = new Item(startTime, byteBuffer, filtersMap);
            queueClient.publish(item).get();
        }

        return Response.status(Response.Status.ACCEPTED).build();
    }

}
