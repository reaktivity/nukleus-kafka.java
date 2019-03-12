/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.control.FreezeFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.Role;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.route.RouteKind;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public final class KafkaController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();
    private final FreezeFW.Builder freezeRW = new FreezeFW.Builder();

    private final KafkaRouteExFW.Builder routeExRW = new KafkaRouteExFW.Builder();

    private final OctetsFW extensionRO = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);

    private final ControllerSpi controllerSpi;
    private final AtomicBuffer commandBuffer;
    private final MutableDirectBuffer extensionBuffer;
    private final Gson gson;

    public KafkaController(
        ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.commandBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.extensionBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.gson = new Gson();
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<KafkaController> kind()
    {
        return KafkaController.class;
    }

    @Override
    public String name()
    {
        return KafkaNukleus.NAME;
    }

    @Deprecated
    public CompletableFuture<Long> routeClient(
        String localAddress,
        String remoteAddress,
        String topic)
    {
        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", topic);
        return route(CLIENT, localAddress, remoteAddress, gson.toJson(extension));
    }

    @Deprecated
    public CompletableFuture<Long> routeClient(
        String localAddress,
        String remoteAddress,
        String topic,
        Map<String, String> headers)
    {
        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", topic);
        extension.add("headers", gson.toJsonTree(headers));

        return route(CLIENT, localAddress, remoteAddress, gson.toJson(extension));
    }

    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress)
    {
        return route(kind, localAddress, remoteAddress, null);
    }

    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        String extension)
    {
        Flyweight routeEx = extensionRO;

        if (extension != null)
        {
            final JsonParser parser = new JsonParser();
            final JsonElement element = parser.parse(extension);
            if (element.isJsonObject())
            {
                final JsonObject object = (JsonObject) element;
                final String topic = gson.fromJson(object.get("topic"), String.class);
                final JsonObject headers = object.getAsJsonObject("headers");

                if (topic != null || headers != null)
                {
                    routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                                       .topicName(topic)
                                       .headers(lhb ->
                                       {
                                           if (headers != null)
                                           {
                                               headers.entrySet().forEach(e ->
                                               {
                                                   final String key = e.getKey();
                                                   final String value = gson.fromJson(e.getValue(), String.class);
                                                   lhb.item(hb -> hb.key(key).value(ob -> ob.put(value.getBytes(UTF_8))));
                                               });
                                           }
                                       })
                                       .build();
                }
            }
        }

        return doRoute(kind, localAddress, remoteAddress, routeEx);
    }

    public CompletableFuture<Void> unroute(
        long routeId)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unrouteRO = unrouteRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                 .correlationId(correlationId)
                                 .nukleus(name())
                                 .routeId(routeId)
                                 .build();

        return controllerSpi.doUnroute(unrouteRO.typeId(), unrouteRO.buffer(), unrouteRO.offset(), unrouteRO.sizeof());
    }

    public CompletableFuture<Void> freeze()
    {
        long correlationId = controllerSpi.nextCorrelationId();

        FreezeFW freeze = freezeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                  .correlationId(correlationId)
                                  .nukleus(name())
                                  .build();

        return controllerSpi.doFreeze(freeze.typeId(), freeze.buffer(), freeze.offset(), freeze.sizeof());
    }

    private CompletableFuture<Long> doRoute(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        Flyweight extension)
    {
        final long correlationId = controllerSpi.nextCorrelationId();
        final Role role = Role.valueOf(kind.ordinal());

        final RouteFW route = routeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                 .correlationId(correlationId)
                                 .nukleus(name())
                                 .role(b -> b.set(role))
                                 .localAddress(localAddress)
                                 .remoteAddress(remoteAddress)
                                 .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                 .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }
}
