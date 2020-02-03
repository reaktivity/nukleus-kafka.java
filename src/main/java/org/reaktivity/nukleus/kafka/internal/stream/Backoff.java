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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.lang.String.format;

public class Backoff
{
    private final int minimum;
    private final int maximum;
    private final int maximumTries;

    Backoff(
        int minimum,
        int maximum)
    {
        assert minimum > 0;
        assert maximum >= minimum;
        this.minimum = minimum;
        this.maximum = maximum;
        this.maximumTries = Integer.numberOfLeadingZeros(minimum) - 1;
    }

    public int next(
        int tries)
    {
        int candidate = maximum;
        if (tries < maximumTries)
        {
            candidate = minimum << tries;
        }
        return Math.min(candidate, maximum);
    }

    @Override
    public String toString()
    {
        return format("Backoff: minimum %d maximum=%d", minimum, maximum);
    }

}
