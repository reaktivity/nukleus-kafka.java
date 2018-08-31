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
package org.reaktivity.nukleus.kafka.internal.util;

import org.agrona.TimerWheel;

import java.util.concurrent.TimeUnit;

public class DelayedTaskScheduler
{
    private final TimerWheel timerWheel;

    public DelayedTaskScheduler()
    {
        this.timerWheel = new TimerWheel(500, TimeUnit.MILLISECONDS, 32);
    }

    public TimerWheel.Timer newTimeout(long delay, Runnable task)
    {
        return timerWheel.newTimeout(delay, TimeUnit.MILLISECONDS, task);
    }

    public void rescheduleTimeout(long delay, TimerWheel.Timer timer)
    {
        timerWheel.rescheduleTimeout(delay, TimeUnit.MILLISECONDS, timer);
    }

    public int process()
    {
        if (timerWheel.computeDelayInMs() == 0)
        {
            return timerWheel.expireTimers();
        }
        return 0;
    }

}
