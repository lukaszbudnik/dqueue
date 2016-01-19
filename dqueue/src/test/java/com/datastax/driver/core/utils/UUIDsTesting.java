/**
 * Copyright (C) 2016 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.datastax.driver.core.utils;

import java.util.UUID;

public class UUIDsTesting {

    public static UUID timeBased(long timestamp) {
        long msb = UUIDs.makeMSB(UUIDs.fromUnixTimestamp(timestamp));
        long lsb = UUIDs.timeBased().getLeastSignificantBits();
        return new UUID(msb, lsb);
    }

}
