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

public interface SequentialQueueClient extends AutoCloseable {

    Future<ImmutableList<UUID>> publishSequential(List<Item> items);

    default Future<ImmutableList<UUID>> publishSequential(Item... items) {
        return publishSequential(Arrays.asList(items));
    }

    Future<Optional<SequentialItem>> consumeSequential(Map<String, String> filters);

    default Future<Optional<SequentialItem>> consumeSequential() {
        return consumeSequential(ImmutableMap.of());
    }

    void delete(SequentialItem item);
}
