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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public final class BackoffTest
{

    @Test
    public void shouldStartWithMinumum()
    {
        Backoff backoff = new Backoff(500, 10_000);
        assertEquals(500, backoff.next(0));
    }
    @Test
    public void test()
    {
        Backoff backoff = new Backoff(1, 10_000);
        assertEquals(10_000, backoff.next(30));
        assertEquals(10_000, backoff.next(31));
        assertEquals(10_000, backoff.next(32));
        assertEquals(10_000, backoff.next(33));
        assertEquals(10_000, backoff.next(40));
    }

    @Test
    public void shouldDoubleUpToMaximum()
    {
        Backoff backoff = new Backoff(500, 10 * 1000);
        int i = 0;
        assertEquals(1000, backoff.next(++i));
        assertEquals(2000, backoff.next(++i));
        assertEquals(4000, backoff.next(++i));
        assertEquals(8000, backoff.next(++i));
        assertEquals(10000, backoff.next(++i));
        assertEquals(10000, backoff.next(31));
        assertEquals(10000, backoff.next(32));
        assertEquals(10000, backoff.next(123456));
    }

    @Test
    public void shouldHandleMaximumLessThanMinimum()
    {
        Backoff backoff = new Backoff(1000, 50);
        assertEquals(50, backoff.next(0));
        assertEquals(50, backoff.next(123));
    }

}
