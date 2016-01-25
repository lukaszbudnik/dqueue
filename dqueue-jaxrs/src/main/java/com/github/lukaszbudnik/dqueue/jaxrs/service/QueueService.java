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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/dqueue/v1")
public class QueueService {

    public static final String X_DQUEUE_START_TIME_HEADER = "X-Dqueue-Start-Time";
    public static final String X_DQUEUE_DEPENDENCY_HEADER = "X-Dqueue-Dependency";
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
                            @FormDataParam("startTime") UUID startTime) throws Exception {
        return publish(contentsInputStream, contentsMetaData, startTime, ImmutableList.of());
    }

    @POST
    @Path("/publish/{filters:.*}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response publish(@FormDataParam("contents") InputStream contentsInputStream,
                         @FormDataParam("contents") FormDataContentDisposition contentsMetaData,
                         @FormDataParam("startTime") UUID startTime,
                         @PathParam("filters") List<PathSegment> filters) throws Exception {
        return publishItem(contentsInputStream, startTime, null, filters);
    }

    @GET
    @Path("/consume")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consume() throws Exception {
        return consume(ImmutableList.of());
    }

    @GET
    @Path("/consume/{filters:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consume(@PathParam("filters") List<PathSegment> filters) throws Exception {
        return consumeItem(filters, false);
    }

    @POST
    @Path("/ordered/publish")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response publishOrdered(@FormDataParam("contents") InputStream contentsInputStream,
                                   @FormDataParam("contents") FormDataContentDisposition contentsMetaData,
                                   @FormDataParam("startTime") UUID startTime,
                                   @FormDataParam("dependency") UUID dependency) throws Exception {
        return publishOrdered(contentsInputStream, contentsMetaData, startTime, dependency, ImmutableList.of());
    }

    @POST
    @Path("/ordered/publish/{filters:.*}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Response publishOrdered(@FormDataParam("contents") InputStream contentsInputStream,
                                   @FormDataParam("contents") FormDataContentDisposition contentsMetaData,
                                   @FormDataParam("startTime") UUID startTime,
                                   @FormDataParam("dependency") UUID dependency,
                                   @PathParam("filters") List<PathSegment> filters) throws Exception {
        return publishItem(contentsInputStream, startTime, dependency, filters);
    }

    @GET
    @Path("/ordered/consume")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consumeOrdered() throws Exception {
        return consumeOrdered(ImmutableList.of());
    }

    @GET
    @Path("/ordered/consume/{filters:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consumeOrdered(@PathParam("filters") List<PathSegment> filters) throws Exception {
        return consumeItem(filters, true);
    }

    @DELETE
    @Path("/ordered/delete/{startTime}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteOrdered(@PathParam("startTime") UUID startTime) {
        return deleteOrdered(startTime, ImmutableList.of());
    }

    @DELETE
    @Path("/ordered/delete/{startTime}/{filters:.*}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteOrdered(@PathParam("startTime") UUID startTime, @PathParam("filters") List<PathSegment> filters) {
        Map<String, String> filtersMap = buildFilters(filters);
        queueClient.deleteOrdered(startTime, filtersMap);
        return Response.status(Response.Status.ACCEPTED).build();
    }

    private Response consumeItem(List<PathSegment> filters, boolean ordered) throws InterruptedException, java.util.concurrent.ExecutionException {
        Map<String, String> filtersMap = buildFilters(filters);

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

    private Map<String, String> buildFilters(List<PathSegment> pathSegments) {
        int size = pathSegments.size();
        if (size == 0) {
            return ImmutableMap.of();
        }
        if (size % 2 != 0) {
            throw new IllegalArgumentException("Filter path segments must be paired 'key/value'");
        }
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int p = 0; p < size; p += 2) {
            builder.put(pathSegments.get(p).getPath(), pathSegments.get(p + 1).getPath());
        }
        return builder.build();
    }

    private Response publishItem(InputStream contentsInputStream, UUID startTime, UUID dependency, List<PathSegment> filters) throws IOException {
        Map<String, String> filtersMap = buildFilters(filters);
        ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(contentsInputStream));
        if (dependency != null) {
            OrderedItem item = new OrderedItem(startTime, dependency, byteBuffer, filtersMap);
            queueClient.publishOrdered(item);
        } else {
            Item item = new Item(startTime, byteBuffer, filtersMap);
            queueClient.publish(item);
        }

        return Response.status(Response.Status.ACCEPTED).build();
    }

}
