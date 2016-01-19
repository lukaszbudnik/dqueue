/**
 * Copyright (C) 2016 Łukasz Budnik <lukasz.budnik@gmail.com>
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
import com.github.lukaszbudnik.dqueue.QueueClient;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.*;
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

    public static final QueueClient queueClient = mock(QueueClient.class);
    public static final QueueService queueService = new QueueService(queueClient);
    public static final Item item = new Item(UUIDs.timeBased(), ByteBuffer.wrap("test".getBytes()));
    public static final Map<String, String> filters = ImmutableMap.of("kkk", "vvv");
    public static final Item itemWithFilters = new Item(UUIDs.timeBased(), ByteBuffer.wrap("test".getBytes()), filters);

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder().
            addResource(queueService).addProvider(MultiPartFeature.class).build();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        when(queueClient.publish(eq(item))).thenReturn(CompletableFuture.completedFuture(item.getStartTime()));
        when(queueClient.consume(argThat(new EmptyMap()))).thenReturn(CompletableFuture.completedFuture(Optional.of(item)));
        when(queueClient.consume(eq(filters))).thenReturn(CompletableFuture.completedFuture(Optional.of(itemWithFilters)));
    }

    @After
    public void after() {
        reset(queueClient);
    }

    @Test
    public void shouldPublishWithoutFilters() {
        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", "test".getBytes(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", item.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        Response publishResponse = resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish").request().buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).publish(eq(item));
    }

    @Test
    public void shouldPublishWithFilters() {
        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", "test".getBytes(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", itemWithFilters.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        Response publishResponse = resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish/kkk/vvv").request().buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).publish(eq(itemWithFilters));
    }

    @Test
    public void shouldRejectPublishWithNotPairedFilters() {

        thrown.expect(new NotPairedPathSegments());

        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", "test".getBytes(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", item.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish/kkk").request().buildPost(entity).invoke();
    }

    @Test
    public void shouldConsumeWithoutFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consume").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString("X-Dqueue-Start-Time");

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(contents, equalTo(item.getContents().array()));
        assertThat(startTime, equalTo(item.getStartTime().toString()));

        verify(queueClient, times(1)).consume(argThat(new EmptyMap()));
    }

    @Test
    public void shouldConsumeWithFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consume/kkk/vvv").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString("X-Dqueue-Start-Time");

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(contents, equalTo(itemWithFilters.getContents().array()));
        assertThat(startTime, equalTo(itemWithFilters.getStartTime().toString()));

        verify(queueClient, times(1)).consume(eq(filters));
    }

    @Test
    public void shouldRejectConsumeWithNotPairedFilters() throws IllegalAccessException, IOException {

        thrown.expect(new NotPairedPathSegments());

        resources.getJerseyTest().client().target("/dqueue/v1/consume/kkk").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();
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
