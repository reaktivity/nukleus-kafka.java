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

import static org.reaktivity.nukleus.route.RouteKind.CLIENT;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.stream.ClientStreamFactoryBuilder;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;

public final class KafkaNukleusFactorySpi implements NukleusFactorySpi
{
    private static final MessagePredicate DEFAULT_ROUTE_HANDLER = (m, b, i, l) ->
    {
        return true;
    };
    private static final Consumer<Consumer<RouteFW>> DEFAULT_ROUTE_CONSUMER = b ->
    {

    };

    private final RouteFW routeRO = new RouteFW();
    private List<Consumer<RouteFW>> topicBootstrappers;

    @Override
    public String name()
    {
        return "kafka";
    }

    @Override
    public Nukleus create(
        Configuration config,
        NukleusBuilder builder)
    {
        KafkaConfiguration kafkaConfig = new KafkaConfiguration(config);

        Consumer<Consumer<RouteFW>> registerTopicBootstrapper = DEFAULT_ROUTE_CONSUMER;
        MessagePredicate routeHandler = DEFAULT_ROUTE_HANDLER;
        if (kafkaConfig.bootstrapEnabled())
        {
            topicBootstrappers = new ArrayList<>();
            registerTopicBootstrapper = topicBootstrappers::add;
            routeHandler = this::handleRouteForBootstrap;
        }

        ClientStreamFactoryBuilder streamFactoryBuilder = new ClientStreamFactoryBuilder(kafkaConfig, registerTopicBootstrapper);

        return builder.streamFactory(CLIENT, streamFactoryBuilder)
                       .routeHandler(CLIENT, routeHandler)
                      .build();
    }

    public boolean handleRouteForBootstrap(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        boolean result = true;
        switch(msgTypeId)
        {
        case RouteFW.TYPE_ID:
            {
                final RouteFW route = routeRO.wrap(buffer, index, index + length);
                topicBootstrappers.forEach(c -> c.accept(route));
            }
            break;
        default:
            break;
        }
        return result;
    }
}
