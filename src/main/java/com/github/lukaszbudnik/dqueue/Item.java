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
