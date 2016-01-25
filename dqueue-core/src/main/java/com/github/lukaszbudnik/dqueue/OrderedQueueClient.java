/**
 * Copyright (C) 2015-2016 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;

public interface OrderedQueueClient extends QueueClient {

    UUID zeroUUID = UUID.fromString("563dde00-219a-11b2-8080-808080808080");

    /**
     * This publish method will convert the list of Items into SequentialItem automatically.
     * The order of sequential items is the same as in the passed list.
     *
     * @param items
     * @return
     */
    Future<ImmutableList<UUID>> publishOrdered(List<Item> items);

    /**
     * Convenient method for simple calls like publish(item1, item2, item3).
     *
     * @param items
     * @return
     */
    default Future<ImmutableList<UUID>> publishOrdered(Item... items) {
        return publishOrdered(Arrays.asList(items));
    }

    /**
     * This variant of publish method takes a single SequentialItem object as an argument.
     * The caller is responsible for correctly setting dependency start time.
     *
     * @param orderedItem
     * @return
     */
    Future<UUID> publishOrdered(OrderedItem orderedItem);

    Future<Optional<OrderedItem>> consumeOrdered(Map<String, String> filters);

    default Future<Optional<OrderedItem>> consumeOrdered() {
        return consumeOrdered(ImmutableMap.of());
    }

    default void deleteOrdered(OrderedItem item) {
        deleteOrdered(item.getStartTime(), item.getFilters());
    }

    void deleteOrdered(UUID startTime, Map<String, String> filters);
}
