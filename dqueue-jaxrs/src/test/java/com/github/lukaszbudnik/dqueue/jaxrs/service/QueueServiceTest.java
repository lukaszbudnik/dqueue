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

import com.datastax.driver.core.utils.UUIDs;
import com.github.lukaszbudnik.dqueue.Item;
import com.github.lukaszbudnik.dqueue.OrderedItem;
import com.github.lukaszbudnik.dqueue.OrderedQueueClient;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class QueueServiceTest {

    public static final OrderedQueueClient queueClient = mock(OrderedQueueClient.class);
    public static final QueueService queueService = new QueueService(queueClient);
    public static final Item item = new Item(UUIDs.timeBased(), ByteBuffer.wrap("test".getBytes()));
    public static final OrderedItem orderedItem = new OrderedItem(item.getStartTime(), OrderedQueueClient.zeroUUID, item.getContents());
    public static final Map<String, String> filters = ImmutableMap.of("kkk", "vvv", "qqq", "www");
    public static final Item itemWithFilters = new Item(UUIDs.timeBased(), item.getContents(), filters);
    public static final OrderedItem orderedItemWithFilters = new OrderedItem(UUIDs.timeBased(), orderedItem.getDependency(), orderedItem.getContents(), filters);

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder().
            addResource(queueService).addProvider(MultiPartFeature.class).build();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        when(queueClient.publish(eq(item))).thenReturn(CompletableFuture.completedFuture(item.getStartTime()));
        when(queueClient.publishOrdered(eq(orderedItem))).thenReturn(CompletableFuture.completedFuture(orderedItem.getStartTime()));
        when(queueClient.consume(argThat(new EmptyMap()))).thenReturn(CompletableFuture.completedFuture(Optional.of(item)));
        when(queueClient.consume(eq(filters))).thenReturn(CompletableFuture.completedFuture(Optional.of(itemWithFilters)));
        when(queueClient.consumeOrdered(argThat(new EmptyMap()))).thenReturn(CompletableFuture.completedFuture(Optional.of(orderedItem)));
        when(queueClient.consumeOrdered(eq(filters))).thenReturn(CompletableFuture.completedFuture(Optional.of(orderedItemWithFilters)));
    }

    @After
    public void after() {
        reset(queueClient);
    }

    @Test
    public void shouldPublishWithoutFilters() {
        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", item.getContents().array(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", item.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        Response publishResponse = resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish").request().buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).publish(eq(item));
    }

    @Test
    public void shouldPublishWithFilters() {
        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", itemWithFilters.getContents().array(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", itemWithFilters.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        Response publishResponse = resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish/kkk/vvv/qqq/www").request().buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).publish(eq(itemWithFilters));
    }

    @Test
    public void shouldPublishOrderedWithoutFilters() {
        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", item.getContents().array(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart()
                .field("startTime", item.getStartTime().toString())
                .field("dependency", OrderedQueueClient.zeroUUID.toString())
                .bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        Response publishResponse = resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publishOrdered").request().buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).publishOrdered(eq(orderedItem));
    }

    @Test
    public void shouldPublishOrderedWithFilters() {
        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", itemWithFilters.getContents().array(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart()
                .field("startTime", itemWithFilters.getStartTime().toString())
                .field("dependency", OrderedQueueClient.zeroUUID.toString())
                .bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        Response publishResponse = resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publishOrdered/kkk/vvv/qqq/www").request().buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        OrderedItem orderedItem = new OrderedItem(itemWithFilters.getStartTime(), OrderedQueueClient.zeroUUID, itemWithFilters.getContents(), itemWithFilters.getFilters());

        verify(queueClient, times(1)).publishOrdered(eq(orderedItem));
    }

    @Test
    public void shouldRejectPublishWithNotPairedFilters() {

        thrown.expect(new NotPairedPathSegments());

        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", "test".getBytes(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", item.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish/kkk/vvv/qqq").request().buildPost(entity).invoke();
    }

    @Test
    public void shouldConsumeWithoutFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consume").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString(QueueService.X_DQUEUE_START_TIME_HEADER);

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(contents, equalTo(item.getContents().array()));
        assertThat(startTime, equalTo(item.getStartTime().toString()));

        verify(queueClient, times(1)).consume(argThat(new EmptyMap()));
    }

    @Test
    public void shouldConsumeWithFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consume/kkk/vvv/qqq/www").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString(QueueService.X_DQUEUE_START_TIME_HEADER);

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(contents, equalTo(itemWithFilters.getContents().array()));
        assertThat(startTime, equalTo(itemWithFilters.getStartTime().toString()));

        verify(queueClient, times(1)).consume(eq(itemWithFilters.getFilters()));
    }

    @Test
    public void shouldConsumeOrderedWithoutFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consumeOrdered").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString(QueueService.X_DQUEUE_START_TIME_HEADER);
        String dependency = clientResponse.getHeaderString(QueueService.X_DQUEUE_DEPENDENCY_HEADER);

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(contents, equalTo(orderedItem.getContents().array()));
        assertThat(startTime, equalTo(orderedItem.getStartTime().toString()));
        assertThat(dependency, equalTo(orderedItem.getDependency().toString()));

        verify(queueClient, times(1)).consumeOrdered(argThat(new EmptyMap()));
    }

    @Test
    public void shouldConsumeOrderedWithFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consumeOrdered/kkk/vvv/qqq/www").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString(QueueService.X_DQUEUE_START_TIME_HEADER);
        String dependency = clientResponse.getHeaderString(QueueService.X_DQUEUE_DEPENDENCY_HEADER);

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(contents, equalTo(orderedItemWithFilters.getContents().array()));
        assertThat(startTime, equalTo(orderedItemWithFilters.getStartTime().toString()));
        assertThat(dependency, equalTo(orderedItemWithFilters.getDependency().toString()));

        verify(queueClient, times(1)).consumeOrdered(eq(orderedItemWithFilters.getFilters()));
    }


    @Test
    public void shouldRejectConsumeWithNotPairedFilters() throws IllegalAccessException, IOException {

        thrown.expect(new NotPairedPathSegments());

        resources.getJerseyTest().client().target("/dqueue/v1/consume/kkk").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();
    }

    @Test
    public void shouldDeleteOrderedWithoutFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/deleteOrdered/" + orderedItem.getStartTime()).request()
                .delete();

        assertThat(response.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).deleteOrdered(eq(orderedItem.getStartTime()), argThat(new EmptyMap()));
    }

    @Test
    public void shouldDeleteOrderedWithFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/deleteOrdered/" + orderedItemWithFilters.getStartTime() + "/kkk/vvv/qqq/www").request()
                .delete();

        assertThat(response.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).deleteOrdered(eq(orderedItemWithFilters.getStartTime()), eq(orderedItemWithFilters.getFilters()));
    }

    @Test
    public void shouldRejectDeleteWithNotPairedFilters() throws IllegalAccessException, IOException {

        thrown.expect(new NotPairedPathSegments());

        resources.getJerseyTest().client().target("/dqueue/v1/deleteOrdered/" + item.getStartTime() + "/kkk").request()
                .delete();
    }

    private class EmptyMap extends ArgumentMatcher<Map> {
        @Override
        public boolean matches(Object argument) {
            return argument instanceof Map && ((Map) argument).isEmpty();
        }
    }

    private class NotPairedPathSegments extends ArgumentMatcher<Exception> {
        @Override
        public boolean matches(Object exception) {
            if (exception instanceof ProcessingException) {
                ProcessingException pe = (ProcessingException)exception;
                return pe.getCause().getMessage().equals("Filter path segments must be paired 'key/value'");
            }
            return false;
        }

    }
}
