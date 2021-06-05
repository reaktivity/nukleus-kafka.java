/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY;

import java.util.function.Function;
import java.util.function.LongFunction;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.config.KafkaBinding;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.function.MessageConsumer;
import org.reaktivity.reaktor.nukleus.stream.StreamFactory;

public final class KafkaCacheServerFactory implements KafkaStreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();

    private final int kafkaTypeId;
    private final Int2ObjectHashMap<StreamFactory> factories;
    private final Long2ObjectHashMap<KafkaBinding> bindings;
    private final KafkaCacheServerAddressFactory cacheAddressFactory;

    public KafkaCacheServerFactory(
        KafkaConfiguration config,
        ElektronContext context,
        Function<String, KafkaCache> supplyCache,
        LongFunction<KafkaCacheRoute> supplyCacheRoute)
    {
        final Long2ObjectHashMap<KafkaBinding> bindings = new Long2ObjectHashMap<>();
        final Int2ObjectHashMap<StreamFactory> factories = new Int2ObjectHashMap<>();

        final KafkaCacheServerBootstrapFactory cacheBootstrapFactory = new KafkaCacheServerBootstrapFactory(
                config, context, bindings::get);

        final KafkaCacheMetaFactory cacheMetaFactory = new KafkaCacheMetaFactory(
                config, context, bindings::get, supplyCache, supplyCacheRoute, (routeId, resolvedId) -> routeId,
                KAFKA_CACHE_SERVER_RECONNECT_DELAY);

        final KafkaCacheServerDescribeFactory cacheDescribeFactory = new KafkaCacheServerDescribeFactory(
                config, context, bindings::get, supplyCache, supplyCacheRoute);

        final KafkaCacheServerFetchFactory cacheFetchFactory = new KafkaCacheServerFetchFactory(
                config, context, bindings::get, supplyCache, supplyCacheRoute);

        final KafkaCacheServerProduceFactory cacheProduceFactory = new KafkaCacheServerProduceFactory(
                config, context, bindings::get, supplyCache, supplyCacheRoute);

        factories.put(KafkaBeginExFW.KIND_BOOTSTRAP, cacheBootstrapFactory);
        factories.put(KafkaBeginExFW.KIND_META, cacheMetaFactory);
        factories.put(KafkaBeginExFW.KIND_DESCRIBE, cacheDescribeFactory);
        factories.put(KafkaBeginExFW.KIND_FETCH, cacheFetchFactory);
        factories.put(KafkaBeginExFW.KIND_PRODUCE, cacheProduceFactory);

        this.kafkaTypeId = context.supplyTypeId(KafkaNukleus.NAME);
        this.factories = factories;
        this.bindings = bindings;

        this.cacheAddressFactory = new KafkaCacheServerAddressFactory(config, context, bindings::get);
    }

    @Override
    public void attach(
        Binding binding)
    {
        KafkaBinding kafkaBinding = new KafkaBinding(binding);
        bindings.put(binding.id, kafkaBinding);

        cacheAddressFactory.onAttached(binding.id);
    }

    @Override
    public void detach(
        long bindingId)
    {
        cacheAddressFactory.onDetached(bindingId);

        bindings.remove(bindingId);
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extension.get(extensionRO::tryWrap);
        assert beginEx != null;
        final int typeId = beginEx.typeId();
        assert beginEx != null && typeId == kafkaTypeId;

        MessageConsumer newStream = null;

        final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::tryWrap);
        if (kafkaBeginEx != null)
        {
            final StreamFactory streamFactory = factories.get(kafkaBeginEx.kind());
            if (streamFactory != null)
            {
                newStream = streamFactory.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), sender);
            }
        }

        return newStream;
    }
}
