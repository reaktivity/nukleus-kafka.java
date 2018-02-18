/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.kafka.internal;

import org.reaktivity.nukleus.Configuration;

public class KafkaConfiguration extends Configuration
{
    public static final String PROPERTY_BROKER_RECONNECT_ATTEMPTS = "nukleus.kafka.maximum.reconnects";
    public static final String PROPERTY_MAXIMUM_UNACKNOWLEDGED_BYTES = "nukleus.kafka.maximum.unacknowledged.bytes";
    public static final String PROPERTY_MAXIMUM_MESSAGE_SIZE  = "nukleus.kafka.maximum.message.size";

    private static final int DEFAULT_BROKER_RECONNECT_ATTEMPTS  = -1; // unlimited
    private static final int DEFAULT_MAXIMUM_UNACKNOWLEDGED_BYTES  = 32 * 1024;
    private static final int DEFAULT_MAXIMUM_MESSAGE_SIZE = 8 * 1024;

    public KafkaConfiguration(
        Configuration config)
    {
        super(config);
    }

    // TODO: do partial decoding and acknowledging of Kafka responses and partial encoding
    //       to avoid the need for this method
    public int  maximumUnacknowledgedBytes()
    {
        return getInteger(PROPERTY_MAXIMUM_UNACKNOWLEDGED_BYTES, DEFAULT_MAXIMUM_UNACKNOWLEDGED_BYTES);
    }

    // TODO: get configured value from Kafka broker
    public int  maximumMessageSize()
    {
        return getInteger(PROPERTY_MAXIMUM_MESSAGE_SIZE, DEFAULT_MAXIMUM_MESSAGE_SIZE);
    }

    public int maximumBrokerReconnects()
    {
        int result = getInteger(PROPERTY_BROKER_RECONNECT_ATTEMPTS, DEFAULT_BROKER_RECONNECT_ATTEMPTS);
        return result == -1 ? Integer.MAX_VALUE : result;
    }


}
