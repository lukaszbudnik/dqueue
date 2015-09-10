package com.github.lukaszbudnik.dqueue.jaxrs.service;

import com.codahale.metrics.annotation.Timed;
import com.github.lukaszbudnik.dqueue.Item;
import com.github.lukaszbudnik.dqueue.QueueClient;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/dqueue/v1")
public class QueueService {

    private final QueueClient queueClient;

    public QueueService(QueueClient queueClient) {
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
        return publish(contentsInputStream, contentsMetaData, startTime, new ArrayList<>());
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
        Map<String, String> filtersMap = buildFilters(filters);
        ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(contentsInputStream));
        Item item = new Item(startTime, byteBuffer, filtersMap);
        queueClient.publish(item);
        return Response.status(Response.Status.ACCEPTED).build();
    }

    @GET
    @Path("/consume")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consume() throws Exception {
        return consume(new ArrayList<>());
    }

    @GET
    @Path("/consume/{filters:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Timed
    public Response consume(@PathParam("filters") List<PathSegment> filters) throws Exception {

        Map<String, String> filtersMap = buildFilters(filters);
        Future<Optional<Item>> itemFuture = queueClient.consume(filtersMap);
        Optional<Item> itemOptional = itemFuture.get();

        Response response = itemOptional.map(i -> {
            StreamingOutput streamingOutput = output -> {
                try {
                    IOUtils.copy(new ByteArrayInputStream(i.getContents().array()), output);
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            };

            return Response.ok(streamingOutput).header("X-Dqueue-Start-Time", UUID.randomUUID().toString()).build();
        }).orElseGet(() -> Response.noContent().build());

        return response;
    }

    private Map<String, String> buildFilters(List<PathSegment> pathSegments) {
        if (pathSegments.size() % 2 != 0) {
            throw new IllegalArgumentException("Filter path segments must be paired 'key/value'");
        }
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int p = 0; p < pathSegments.size() / 2; p += 2) {
            builder.put(pathSegments.get(p).getPath(), pathSegments.get(p + 1).getPath());
        }
        return builder.build();
    }

}
