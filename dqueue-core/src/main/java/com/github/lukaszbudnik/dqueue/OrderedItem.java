/**
 * Copyright (C) 2015-2017 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue;

import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class OrderedItem extends Item {
    private final UUID dependency;

    public OrderedItem(UUID startTime, UUID dependency, ByteBuffer contents) {
        this(startTime, dependency, contents, ImmutableMap.of());
    }

    public OrderedItem(UUID startTime, UUID dependency, ByteBuffer contents, Map<String, ?> filters) {
        super(startTime, contents, filters);
        this.dependency = dependency;
    }

    public UUID getDependency() {
        return dependency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        OrderedItem that = (OrderedItem) o;

        return !(dependency != null ? !dependency.equals(that.dependency) : that.dependency != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getDependency() != null ? getDependency().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SequentialItem{" +
                "startTime=" + getStartTime() +
                ", dependency=" + dependency +
                ", filters=" + getFilters() +
                ", contents=" + getContents() +
                '}';
    }
}
