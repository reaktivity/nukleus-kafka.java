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
    private final int maxTcpReadBytes;

    public KafkaConfiguration(
        Configuration config)
    {
        super(config);
        maxTcpReadBytes = getInteger(org.reaktivity.reaktor.internal.ReaktorConfiguration.MEMORY_BLOCK_CAPACITY_PROPERTY,
                org.reaktivity.reaktor.internal.ReaktorConfiguration.MEMORY_BLOCK_CAPACITY_DEFAULT);
    }

    // TODO: do partial decoding and acknowledging of Kafka responses and partial encoding
    //       to avoid the need for this method
    public int  maximumUnacknowledgedBytes()
    {
        return maxTcpReadBytes;
    }
}
