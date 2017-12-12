/**
 * Copyright (C) 2015-2017 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue.jaxrs.service;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.glassfish.jersey.client.ClientResponse;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueueServiceTest extends AbstractQueueServiceTest {

    @Test
    public void shouldPublishNoFilters() {
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
                .target("/dqueue/v1/publish")
                .request()
                .header(QueueService.X_DQUEUE_FILTERS, filtersHeader)
                .buildPost(entity).invoke();

        assertThat(publishResponse.getStatus(), equalTo(Response.Status.ACCEPTED.getStatusCode()));

        verify(queueClient, times(1)).publish(eq(itemWithFilters));
    }

    @Test
    public void shouldRejectPublishWithNotPairedFilters1() {

        thrown.expect(new NotPairedPathSegments());

        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", "test".getBytes(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", item.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish")
                .request()
                .header(QueueService.X_DQUEUE_FILTERS, "kkk=vvv,qqq")
                .buildPost(entity)
                .invoke();
    }

    @Test
    public void shouldRejectPublishWithNotPairedFilters2() {

        thrown.expect(new NotPairedPathSegments());

        FormDataBodyPart formDataPart = new FormDataBodyPart("contents", "test".getBytes(), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        MultiPart multiPart = new FormDataMultiPart().field("startTime", item.getStartTime().toString()).bodyPart(formDataPart);

        Entity entity = Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE);

        resources.client().register(MultiPartFeature.class)
                .target("/dqueue/v1/publish")
                .request()
                .header(QueueService.X_DQUEUE_FILTERS, "kkk=vvv,qqq=")
                .buildPost(entity)
                .invoke();
    }

    @Test
    public void shouldConsumeNoFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consume").request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString(QueueService.X_DQUEUE_START_TIME_HEADER);
        String filters = clientResponse.getHeaderString(QueueService.X_DQUEUE_FILTERS);
        String cacheControl = clientResponse.getHeaderString(HttpHeaders.CACHE_CONTROL);

        assertThat(filters, equalTo(""));
        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(cacheControl, equalTo("no-cache"));
        assertThat(contents, equalTo(item.getContents().array()));
        assertThat(startTime, equalTo(item.getStartTime().toString()));

        verify(queueClient, times(1)).consume(argThat(new EmptyMap()));
    }

    @Test
    public void shouldConsumeWithFilters() throws IllegalAccessException, IOException {
        Response response = resources.getJerseyTest().client().target("/dqueue/v1/consume")
                .request()
                .header(QueueService.X_DQUEUE_FILTERS, filtersHeader)
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

        ClientResponse clientResponse = (ClientResponse) FieldUtils.readField(response, "context", true);

        ByteArrayInputStream bais = (ByteArrayInputStream)response.getEntity();
        byte[] contents = IOUtils.toByteArray(bais);
        String startTime = clientResponse.getHeaderString(QueueService.X_DQUEUE_START_TIME_HEADER);
        String filters = clientResponse.getHeaderString(QueueService.X_DQUEUE_FILTERS);
        String cacheControl = clientResponse.getHeaderString(HttpHeaders.CACHE_CONTROL);

        assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
        assertThat(cacheControl, equalTo("no-cache"));
        assertThat(filters, equalTo(filtersHeader));
        assertThat(contents, equalTo(itemWithFilters.getContents().array()));
        assertThat(startTime, equalTo(itemWithFilters.getStartTime().toString()));

        verify(queueClient, times(1)).consume(eq(itemWithFilters.getFilters()));
    }

    @Test
    public void shouldRejectConsumeWithNotPairedFilters() throws IllegalAccessException, IOException {

        thrown.expect(new NotPairedPathSegments());

        resources.getJerseyTest().client().target("/dqueue/v1/consume")
                .request()
                .header(QueueService.X_DQUEUE_FILTERS, "kkk=vvv,qqq=")
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();
    }

}
