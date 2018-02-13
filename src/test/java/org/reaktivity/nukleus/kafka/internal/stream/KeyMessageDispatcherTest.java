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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordFW;

public final class KeyMessageDispatcherTest
{
    private KeyMessageDispatcher dispatcher = new KeyMessageDispatcher();

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAddNewDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        assertNull(dispatcher.addIfAbsent(asOctets("key_1"), child1));
        assertNull(dispatcher.addIfAbsent(asOctets("key_2"), child2));
    }

    @Test
    public void shouldReturnExistingDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher child3 = context.mock(MessageDispatcher.class, "child3");
        dispatcher.addIfAbsent(asOctets("key_1"), child1);
        dispatcher.addIfAbsent(asOctets("key_2"), child2);
        assertSame(child1, dispatcher.addIfAbsent(asOctets("key_1"), child3));
        assertSame(child2, dispatcher.addIfAbsent(asOctets("key_2"), child3));
    }

    @Test
    public void shouldDispatch()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        assertNull(dispatcher.addIfAbsent(asOctets("key_1"), child1));
        assertNull(dispatcher.addIfAbsent(asOctets("key_2"), child2));
        final RecordFW record = new RecordFW.Builder().wrap(new UnsafeBuffer(new byte[100]), 11, 100)
                .length(50)
                .attributes((byte) 0)
                .timestampDelta(0)
                .offsetDelta(0)
                .keyLen(5)
                .key(asOctets("key_2"))
                .valueLen(3)
                .value(asOctets("123"))
                .headerCount(0)
                .build();
        context.checking(new Expectations()
        {
            {
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with(record));
                will(returnValue(1));
            }
        });
        assertEquals(1, dispatcher.dispatch(1, 10, 12, record));
    }

    @Test
    public void shouldNotDispatch()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        assertNull(dispatcher.addIfAbsent(asOctets("key_1"), child1));
        assertNull(dispatcher.addIfAbsent(asOctets("key_2"), child2));
        final RecordFW record = new RecordFW.Builder().wrap(new UnsafeBuffer(new byte[100]), 11, 100)
                .length(50)
                .attributes((byte) 0)
                .timestampDelta(0)
                .offsetDelta(0)
                .keyLen(5)
                .key(asOctets("key_3"))
                .valueLen(3)
                .value(asOctets("123"))
                .headerCount(0)
                .build();
        assertEquals(0, dispatcher.dispatch(1, 10, 12, record));
    }

    private OctetsFW asOctets(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new OctetsFW.Builder().wrap(new UnsafeBuffer(bytes), 0, bytes.length)
                .put(bytes)
                .build();
    }

}
