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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaMergeableFetchFactory implements StreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();

    private final int kafkaTypeId;
    private final StreamFactory fetchFactory;
    private final StreamFactory mergedFetchFactory;

    public KafkaMergeableFetchFactory(
        ToIntFunction<String> supplyTypeId,
        StreamFactory fetchFactory,
        StreamFactory mergedFetchFactory)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.fetchFactory = fetchFactory;
        this.mergedFetchFactory = mergedFetchFactory;
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
        final long initialId = begin.streamId();

        assert (initialId & 0x0000_0000_0000_0001L) != 0L;

        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extension.get(extensionRO::tryWrap);
        assert beginEx != null && beginEx.typeId() == kafkaTypeId;
        final KafkaBeginExFW kafkaBeginEx = extension.get(kafkaBeginExRO::tryWrap);
        assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_FETCH;
        final KafkaFetchBeginExFW kafkaFetchBeginEx = kafkaBeginEx.fetch();
        final ArrayFW<KafkaOffsetFW> progress = kafkaFetchBeginEx.progress();

        MessageConsumer newStream = null;

        final KafkaOffsetFW partition = progress.matchFirst(p -> p.partitionId() == -1);
        if (partition != null)
        {
            newStream = mergedFetchFactory.newStream(msgTypeId, buffer, index, length, sender);
        }

        if (newStream == null)
        {
            newStream = fetchFactory.newStream(msgTypeId, buffer, index, length, sender);
        }

        return newStream;
    }
}
