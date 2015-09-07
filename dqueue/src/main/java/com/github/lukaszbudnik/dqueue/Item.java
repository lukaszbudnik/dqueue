/**
 * Copyright (C) 2015 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class Item {
    private final UUID startTime;
    private final ByteBuffer contents;
    private final Map<String, ?> filters;

    public Item(UUID startTime, ByteBuffer contents) {
        this(startTime, contents, Collections.emptyMap());
    }

    public Item(UUID startTime, ByteBuffer contents, Map<String, ?> filters) {
        this.startTime = startTime;
        this.contents = contents;
        this.filters = filters;
    }

    public UUID getStartTime() {
        return startTime;
    }

    public ByteBuffer getContents() {
        return contents;
    }

    public Map<String, ?> getFilters() {
        return filters;
    }
}
