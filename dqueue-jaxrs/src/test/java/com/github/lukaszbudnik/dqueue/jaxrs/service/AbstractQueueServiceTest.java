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
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.ProcessingException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class AbstractQueueServiceTest {
    public static final OrderedQueueClient queueClient = mock(OrderedQueueClient.class);
    public static final QueueService queueService = new QueueService(queueClient);
    public static final Item item = new Item(UUIDs.timeBased(), ByteBuffer.wrap("test".getBytes()));
    public static final OrderedItem orderedItem = new OrderedItem(item.getStartTime(), OrderedQueueClient.zeroUUID, item.getContents());
    public static final Map<String, Object> filters = ImmutableMap.of("kkk", "vvv", "qqq", "www");
    public static final String filtersHeader = "kkk=vvv,qqq=www";
    public static final Item itemWithFilters = new Item(UUIDs.timeBased(), item.getContents(), filters);
    public static final OrderedItem orderedItemWithFilters = new OrderedItem(itemWithFilters.getStartTime(), orderedItem.getDependency(), orderedItem.getContents(), filters);
    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder().
            addResource(queueService).addProvider(MultiPartFeature.class).build();
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        when(queueClient.publish(any(Item.class))).thenReturn(CompletableFuture.completedFuture(item.getStartTime()));
        when(queueClient.consume(argThat(new EmptyMap()))).thenReturn(CompletableFuture.completedFuture(Optional.of(item)));
        when(queueClient.consume(eq(filters))).thenReturn(CompletableFuture.completedFuture(Optional.of(itemWithFilters)));

        when(queueClient.publishOrdered(any(OrderedItem.class))).thenReturn(CompletableFuture.completedFuture(orderedItem.getStartTime()));
        when(queueClient.consumeOrdered(argThat(new EmptyMap()))).thenReturn(CompletableFuture.completedFuture(Optional.of(orderedItem)));
        when(queueClient.consumeOrdered(eq(filters))).thenReturn(CompletableFuture.completedFuture(Optional.of(orderedItemWithFilters)));

    }

    @After
    public void after() {
        reset(queueClient);
    }

    protected class EmptyMap extends ArgumentMatcher<Map> {
        @Override
        public boolean matches(Object argument) {
            return argument instanceof Map && ((Map) argument).isEmpty();
        }
    }

    protected class NotPairedPathSegments extends ArgumentMatcher<Exception> {
        @Override
        public boolean matches(Object exception) {
            if (exception instanceof ProcessingException) {
                ProcessingException pe = (ProcessingException)exception;
                return pe.getCause().getMessage().equals("Filter path segments must be paired 'key=value'");
            }
            return false;
        }
    }
}
