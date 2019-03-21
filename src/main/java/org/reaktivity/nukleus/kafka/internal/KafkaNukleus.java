/**
 * Copyright 2016-2019 The Reaktivity Project
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

import static org.reaktivity.nukleus.route.RouteKind.CLIENT;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteKind;

final class KafkaNukleus implements Nukleus
{
    static final String NAME = "kafka";

    private final KafkaConfiguration config;
    private final KafkaAgent agent;
    private final Map<RouteKind, MessagePredicate> routeHandlers;

    private final AtomicInteger elektrons;

    KafkaNukleus(
        KafkaConfiguration config)
    {
        this.config = config;
        this.agent = new KafkaAgent(config);
        this.routeHandlers = Collections.singletonMap(CLIENT, agent::handleRoute);
        this.elektrons = new AtomicInteger();
    }

    @Override
    public String name()
    {
        return KafkaNukleus.NAME;
    }

    @Override
    public KafkaConfiguration config()
    {
        return config;
    }

    @Override
    public MessagePredicate routeHandler(
        RouteKind kind)
    {
        return routeHandlers.get(kind);
    }

    @Override
    public KafkaElektron supplyElektron()
    {
        if (elektrons.incrementAndGet() > 1)
        {
            throw new IllegalStateException("multiple KafkaElektrons not yet supported");
        }

        return new KafkaElektron(config, agent);
    }
}
