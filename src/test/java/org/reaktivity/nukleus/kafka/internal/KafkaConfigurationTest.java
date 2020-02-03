/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_READ_IDLE_TIMEOUT;

import org.junit.Test;

public class KafkaConfigurationTest
{
    // needed by test annotations
    public static final String KAFKA_READ_IDLE_TIMEOUT_NAME = "nukleus.kafka.read.idle.timeout";

    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(KAFKA_READ_IDLE_TIMEOUT.name(), KAFKA_READ_IDLE_TIMEOUT_NAME);
    }
}
