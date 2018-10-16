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
package org.reaktivity.nukleus.kafka.internal.stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public final class MessageDispatcherTest
{
    @Test
    public void shouldDetectFlagsDelivered()
    {
        assertFalse(MessageDispatcher.expectingWindow(0x01));
        assertFalse(MessageDispatcher.delivered(0x01));
        assertFalse(MessageDispatcher.expectingWindow(0x02));
        assertFalse(MessageDispatcher.delivered(0x02));
        assertFalse(MessageDispatcher.delivered(0x03));
        assertTrue(MessageDispatcher.expectingWindow(0x03));
        assertFalse(MessageDispatcher.expectingWindow(0x05));
        assertFalse(MessageDispatcher.delivered(0x05));
        assertTrue(MessageDispatcher.delivered(0x07));
    }

    @Test
    public void shouldDetectFlagsMatched()
    {
        assertTrue(MessageDispatcher.matched(0x01));
        assertTrue(MessageDispatcher.matched(0x03));
        assertFalse(MessageDispatcher.matched(0x02));
    }
}
