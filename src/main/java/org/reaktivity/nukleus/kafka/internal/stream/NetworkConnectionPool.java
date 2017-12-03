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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.LongIterator;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.function.PartitionResponseConsumer;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;

final class NetworkConnectionPool
{
    final RequestHeaderFW.Builder requestRW = new RequestHeaderFW.Builder();
    final FetchRequestFW.Builder fetchRequestRW = new FetchRequestFW.Builder();
    final TopicRequestFW.Builder topicRequestRW = new TopicRequestFW.Builder();
    final PartitionRequestFW.Builder partitionRequestRW = new PartitionRequestFW.Builder();
    final OctetsFW.Builder payloadRW = new OctetsFW.Builder();
    final MutableDirectBuffer encodeBuffer;

    final WindowFW windowRO = new WindowFW();
    final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    final TopicResponseFW topicResponseRO = new TopicResponseFW();
    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();


    private final ClientStreamFactory clientStreamFactory;
    private final String networkName;
    private final long networkRef;
    private final BufferPool bufferPool;
    private final NetworkConnection connection;

    NetworkConnectionPool(
        ClientStreamFactory clientStreamFactory,
        String networkName,
        long networkRef,
        BufferPool bufferPool)
    {
        this.clientStreamFactory = clientStreamFactory;
        this.networkName = networkName;
        this.networkRef = networkRef;
        this.bufferPool = bufferPool;
        this.connection = new NetworkConnection();
        this.encodeBuffer = new UnsafeBuffer(new byte[clientStreamFactory.bufferPool.slotCapacity()]);
    }

    long doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        PartitionResponseConsumer consumeRecords,
        IntSupplier supplyWindow)
    {
        // TODO: get topic metadata, split fetchOffsets into one set per broker (connection),
        final int connectionId = 0;
        final int connectionAttachId = connection.doAttach(topicName, fetchOffsets, consumeRecords, supplyWindow);

        return ((long) connectionId) << 32 | connectionAttachId;
    }

    void doFlush(
        long connectionPoolAttachId)
    {
        int connectionId = (int)((connectionPoolAttachId >> 32) & 0xFFFF_FFFF);

        // TODO: connection pool size > 1
        assert connectionId == 0;

        connection.doFlush();
    }

    void doDetach(
        long connectionPoolAttachId,
        Long2LongHashMap fetchOffsets)
    {
        int connectionId = (int)((connectionPoolAttachId >> 32) & 0xFFFF_FFFF);
        int connectionAttachId = (int)(connectionPoolAttachId & 0xFFFF_FFFF);

        // TODO: connection pool size > 1
        assert connectionId == 0;

        connection.doDetach(connectionAttachId, fetchOffsets);
    }

    final class NetworkConnection
    {
        private final MessageConsumer networkTarget;
        private final Map<String, NetworkTopic> topicsByName;
        private final Int2ObjectHashMap<Consumer<Long2LongHashMap>> detachersById;

        private int nextAttachId;
        private int nextRequestId;
        private int nextResponseId;

        private long networkId;

        private int networkRequestBudget;
        private int networkRequestPadding;

        private int networkResponseBudget;
        private int networkResponsePadding;

        private MessageConsumer networkReplyThrottle;
        private long networkReplyId;

        private MessageConsumer streamState;

        private int networkSlot = NO_SLOT;
        private int networkSlotOffset;

        private NetworkConnection()
        {
            this.networkTarget = NetworkConnectionPool.this.clientStreamFactory.router.supplyTarget(networkName);
            this.topicsByName = new LinkedHashMap<>();
            this.detachersById = new Int2ObjectHashMap<>();
        }

        @Override
        public String toString()
        {
            return String.format("[budget=%d, padding=%d]", networkRequestBudget, networkRequestPadding);
        }

        MessageConsumer onCorrelated(
            MessageConsumer networkReplyThrottle,
            long networkReplyId)
        {
            this.networkReplyThrottle = networkReplyThrottle;
            this.networkReplyId = networkReplyId;
            this.streamState = this::beforeBegin;

            return this::handleStream;
        }

        private int doAttach(
            String topicName,
            Long2LongHashMap fetchOffsets,
            PartitionResponseConsumer consumeRecords,
            IntSupplier supplyWindow)
        {
            final NetworkTopic topic = topicsByName.computeIfAbsent(topicName, NetworkTopic::new);
            topic.doAttach(fetchOffsets, consumeRecords, supplyWindow);

            final int newAttachId = nextAttachId++;

            detachersById.put(newAttachId, f -> topic.doDetach(f, consumeRecords, supplyWindow));

            doFetchIfNotInFlight();

            return newAttachId;
        }

        private void doFlush()
        {
            doFetchIfNotInFlight();
        }

        private void doDetach(
            int attachId,
            Long2LongHashMap fetchOffsets)
        {
            final Consumer<Long2LongHashMap> detacher = detachersById.remove(attachId);
            if (detacher != null)
            {
                detacher.accept(fetchOffsets);
            }
        }

        private void doFetchIfNotInFlight()
        {
            if (nextRequestId == nextResponseId)
            {
                doBeginIfNotConnected();

                if (networkRequestBudget > networkRequestPadding)
                {
                    final int encodeOffset = 512;
                    int encodeLimit = encodeOffset;

                    RequestHeaderFW request = requestRW.wrap(
                            NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                            NetworkConnectionPool.this.encodeBuffer.capacity())
                            .size(0)
                            .apiKey(ClientStreamFactory.FETCH_API_KEY)
                            .apiVersion(ClientStreamFactory.FETCH_API_VERSION)
                            .correlationId(0)
                            .clientId((String) null)
                            .build();

                    encodeLimit = request.limit();

                    FetchRequestFW fetchRequest = fetchRequestRW.wrap(
                            NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                            NetworkConnectionPool.this.encodeBuffer.capacity())
                            .maxWaitTimeMillis(500)
                            .minBytes(1)
                            .maxBytes(0)
                            .isolationLevel((byte) 0)
                            .topicCount(0)
                            .build();

                    encodeLimit = fetchRequest.limit();

                    int maxFetchBytes = 0;
                    int topicCount = 0;
                    for (String topicName : topicsByName.keySet())
                    {
                        TopicRequestFW topicRequest = topicRequestRW.wrap(
                                NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                NetworkConnectionPool.this.encodeBuffer.capacity())
                                .name(topicName)
                                .partitionCount(0)
                                .build();

                        encodeLimit = topicRequest.limit();

                        NetworkTopic topic = topicsByName.get(topicName);
                        int maxPartitionBytes = topic.maximumWritableBytes();

                        maxFetchBytes += maxPartitionBytes;

                        int partitionCount = 0;
                        int partitionId = -1;
                        // TODO: eliminate iterator allocation
                        for (NetworkTopicPartition partition : topic.partitions)
                        {
                            if (partitionId < partition.id)
                            {
                                PartitionRequestFW partitionRequest = NetworkConnectionPool.this.partitionRequestRW
                                    .wrap(NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                            NetworkConnectionPool.this.encodeBuffer.capacity())
                                    .partitionId(partition.id)
                                    .fetchOffset(partition.offset)
                                    .maxBytes(maxPartitionBytes)
                                    .build();

                                encodeLimit = partitionRequest.limit();
                                partitionId = partition.id;
                                partitionCount++;
                            }
                        }

                        NetworkConnectionPool.this.topicRequestRW
                                      .wrap(NetworkConnectionPool.this.encodeBuffer, topicRequest.offset(), topicRequest.limit())
                                      .name(topicRequest.name())
                                      .partitionCount(partitionCount)
                                      .build();
                        topicCount++;
                    }

                    NetworkConnectionPool.this.fetchRequestRW
                                  .wrap(NetworkConnectionPool.this.encodeBuffer, fetchRequest.offset(), fetchRequest.limit())
                                  .maxWaitTimeMillis(fetchRequest.maxWaitTimeMillis())
                                  .minBytes(fetchRequest.minBytes())
                                  .maxBytes(maxFetchBytes)
                                  .isolationLevel(fetchRequest.isolationLevel())
                                  .topicCount(topicCount)
                                  .build();

                    // TODO: stream large requests in multiple DATA frames as needed
                    if (maxFetchBytes > 0 && encodeLimit - encodeOffset + networkRequestPadding <= networkRequestBudget)
                    {
                        int newCorrelationId = nextRequestId++;

                        NetworkConnectionPool.this.requestRW
                                 .wrap(NetworkConnectionPool.this.encodeBuffer, request.offset(), request.limit())
                                 .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                                 .apiKey(ClientStreamFactory.FETCH_API_KEY)
                                 .apiVersion(ClientStreamFactory.FETCH_API_VERSION)
                                 .correlationId(newCorrelationId)
                                 .clientId((String) null)
                                 .build();

                        OctetsFW payload = NetworkConnectionPool.this.payloadRW
                                .wrap(NetworkConnectionPool.this.encodeBuffer, encodeOffset, encodeLimit)
                                .set((b, o, m) -> m - o)
                                .build();

                        NetworkConnectionPool.this.clientStreamFactory.doData(networkTarget, networkId, payload);
                        networkRequestBudget -= payload.sizeof() + networkRequestPadding;
                    }
                }
            }
        }

        private void doBeginIfNotConnected()
        {
            if (networkId == 0L)
            {
                // TODO: progressive back-off before reconnect
                //       if choose to give up, say after maximum retry attempts,
                //       then send END to each consumer to clean up

                final long newNetworkId = NetworkConnectionPool.this.clientStreamFactory.supplyStreamId.getAsLong();
                final long newCorrelationId = NetworkConnectionPool.this.clientStreamFactory.supplyCorrelationId.getAsLong();

                NetworkConnectionPool.this.clientStreamFactory.correlations.put(newCorrelationId, NetworkConnection.this);

                NetworkConnectionPool.this.clientStreamFactory.doBegin(networkTarget, newNetworkId, networkRef, newCorrelationId);
                NetworkConnectionPool.this.clientStreamFactory.router.setThrottle(
                        networkName, newNetworkId, this::handleThrottle);

                this.networkId = newNetworkId;
            }
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = NetworkConnectionPool.this.windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = NetworkConnectionPool.this.clientStreamFactory.resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleWindow(
            final WindowFW window)
        {
            final int networkWindowCredit = window.credit();
            final int networkWindowPadding = window.padding();

            this.networkRequestBudget += networkWindowCredit;
            this.networkRequestPadding = networkWindowPadding;

            doFetchIfNotInFlight();
        }

        private void handleReset(
            ResetFW reset)
        {
            doReinitialize();
            doFetchIfNotInFlight();
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = NetworkConnectionPool.this.clientStreamFactory.beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                NetworkConnectionPool.this.clientStreamFactory.doReset(networkReplyThrottle, networkReplyId);
            }
        }

        private void afterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = NetworkConnectionPool.this.clientStreamFactory.dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = NetworkConnectionPool.this.clientStreamFactory.endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = NetworkConnectionPool.this.clientStreamFactory.abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                NetworkConnectionPool.this.clientStreamFactory.doReset(networkReplyThrottle, networkReplyId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            doOfferResponseBudget();

            this.streamState = this::afterBegin;
        }

        private void handleData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();

            networkResponseBudget -= payload.sizeof() + networkResponsePadding;

            if (networkResponseBudget < 0)
            {
                NetworkConnectionPool.this.clientStreamFactory.doReset(networkReplyThrottle, networkReplyId);
            }
            else
            {
                try
                {
                    DirectBuffer networkBuffer = payload.buffer();
                    int networkOffset = payload.offset();
                    int networkLimit = payload.limit();

                    if (networkSlot != NO_SLOT)
                    {
                        final MutableDirectBuffer bufferSlot =
                                NetworkConnectionPool.this.clientStreamFactory.bufferPool.buffer(networkSlot);
                        final int networkRemaining = networkLimit - networkOffset;

                        bufferSlot.putBytes(networkSlotOffset, networkBuffer, networkOffset, networkRemaining);
                        networkSlotOffset += networkRemaining;

                        networkBuffer = bufferSlot;
                        networkOffset = 0;
                        networkLimit = networkSlotOffset;
                    }

                    ResponseHeaderFW response = null;
                    if (networkOffset + BitUtil.SIZE_OF_INT <= networkLimit)
                    {
                        response = NetworkConnectionPool.this.responseRO.wrap(networkBuffer, networkOffset, networkLimit);
                        networkOffset = response.limit();
                    }

                    if (response == null || networkOffset + response.size() > networkLimit)
                    {
                        if (networkSlot == NO_SLOT)
                        {
                            networkSlot = NetworkConnectionPool.this.clientStreamFactory.bufferPool.acquire(networkReplyId);

                            final MutableDirectBuffer bufferSlot =
                                    NetworkConnectionPool.this.clientStreamFactory.bufferPool.buffer(networkSlot);
                            bufferSlot.putBytes(networkSlotOffset, payload.buffer(), payload.offset(), payload.sizeof());
                            networkSlotOffset += payload.sizeof();
                        }
                    }
                    else
                    {
                        handleFetchResponse(networkBuffer, networkOffset, networkLimit);
                        networkOffset += response.size();
                        nextResponseId++;

                        if (networkOffset < networkLimit)
                        {
                            if (networkSlot == NO_SLOT)
                            {
                                networkSlot = NetworkConnectionPool.this.clientStreamFactory.bufferPool.acquire(networkReplyId);
                            }

                            final MutableDirectBuffer bufferSlot =
                                    NetworkConnectionPool.this.clientStreamFactory.bufferPool.buffer(networkSlot);
                            bufferSlot.putBytes(0, networkBuffer, networkOffset, networkLimit - networkOffset);
                            networkSlotOffset = networkLimit - networkOffset;
                        }
                        else
                        {
                            assert networkOffset == networkLimit;
                            networkSlotOffset = 0;
                        }

                        doOfferResponseBudget();
                        doFetchIfNotInFlight();
                    }
                }
                finally
                {
                    if (networkSlotOffset == 0 && networkSlot != NO_SLOT)
                    {
                        NetworkConnectionPool.this.clientStreamFactory.bufferPool.release(networkSlot);
                        networkSlot = NO_SLOT;
                    }
                }
            }
        }

        private void handleFetchResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            int networkLimit)
        {
            final FetchResponseFW fetchResponse =
                    NetworkConnectionPool.this.fetchResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
            final int topicCount = fetchResponse.topicCount();

            networkOffset = fetchResponse.limit();
            for (int topicIndex = 0; topicIndex < topicCount; topicIndex++)
            {
                final TopicResponseFW topicResponse =
                        NetworkConnectionPool.this.topicResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                final String topicName = topicResponse.name().asString();
                final int partitionCount = topicResponse.partitionCount();

                networkOffset = topicResponse.limit();

                final NetworkTopic topic = topicsByName.get(topicName);
                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
                {
                    final PartitionResponseFW partitionResponse =
                            NetworkConnectionPool.this.partitionResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                    networkOffset = partitionResponse.limit();

                    final RecordSetFW recordSet =
                            NetworkConnectionPool.this.recordSetRO.wrap(networkBuffer, networkOffset, networkLimit);
                    final int recordBatchSize = recordSet.recordBatchSize();
                    networkOffset = recordSet.limit() + recordBatchSize;

                    if (topic != null)
                    {
                        int partitionResponseSize = networkOffset - partitionResponse.offset();

                        topic.onPartitionResponse(partitionResponse.buffer(),
                                                  partitionResponse.offset(),
                                                  partitionResponseSize);
                    }
                }
            }
        }

        private void handleEnd(
            EndFW end)
        {
            doReinitialize();
            doFetchIfNotInFlight();
        }

        private void handleAbort(
            AbortFW abort)
        {
            doReinitialize();
            doFetchIfNotInFlight();
        }

        private void doReinitialize()
        {
            networkRequestBudget = 0;
            networkRequestPadding = 0;
            networkId = 0L;
            nextRequestId = 0;
            nextResponseId = 0;
        }

        private void doOfferResponseBudget()
        {
            final int networkResponseCredit =
                    Math.max(NetworkConnectionPool.this.bufferPool.slotCapacity() - networkSlotOffset - networkResponseBudget, 0);

            NetworkConnectionPool.this.clientStreamFactory.doWindow(
                    networkReplyThrottle, networkReplyId, networkResponseCredit, networkResponsePadding);

            this.networkResponseBudget += networkResponseCredit;
        }
    }

    final class NetworkTopic
    {
        private final String topicName;
        private final Set<PartitionResponseConsumer> recordConsumers;
        private final Set<IntSupplier> windowSuppliers;
        final NavigableSet<NetworkTopicPartition> partitions;
        private final NetworkTopicPartition candidate;
        private final PartitionProgressHandler progressHandler;

        @Override
        public String toString()
        {
            return String.format("topicName=%s, partitions=%s", topicName, partitions);
        }

        NetworkTopic(
            String topicName)
        {
            this.topicName = topicName;
            this.recordConsumers = new HashSet<>();
            this.windowSuppliers = new HashSet<>();
            this.partitions = new TreeSet<>();
            this.candidate = new NetworkTopicPartition();
            this.progressHandler = this::handleProgress;
        }

        void doAttach(
            Long2LongHashMap fetchOffsets,
            PartitionResponseConsumer consumeRecords,
            IntSupplier supplyWindow)
        {
            recordConsumers.add(consumeRecords);
            windowSuppliers.add(supplyWindow);
            candidate.id = -1;

            final LongIterator iterator = fetchOffsets.values().iterator();
            while (iterator.hasNext())
            {
                candidate.id++;
                candidate.offset = iterator.nextValue();

                NetworkTopicPartition partition = partitions.floor(candidate);
                if (partition == null || partition.id != candidate.id)
                {
                    partition = new NetworkTopicPartition();
                    partition.id = candidate.id;
                    partition.offset = candidate.offset;

                    partitions.add(partition);
                }

                partition.refs++;
            }
        }

        void doDetach(
            Long2LongHashMap fetchOffsets,
            PartitionResponseConsumer consumeRecords,
            IntSupplier supplyWindow)
        {
            recordConsumers.remove(consumeRecords);
            windowSuppliers.remove(supplyWindow);
            candidate.id = -1;

            final LongIterator iterator = fetchOffsets.values().iterator();
            while (iterator.hasNext())
            {
                candidate.id++;
                candidate.offset = iterator.nextValue();

                NetworkTopicPartition partition = partitions.floor(candidate);
                if (partition != null)
                {
                    partition.refs--;

                    if (partition.refs == 0)
                    {
                        partitions.remove(partition);
                    }
                }
            }
        }

        void onPartitionResponse(
            DirectBuffer buffer,
            int index,
            int length)
        {
            // TODO: parse here, callback for individual records only
            //       via RecordConsumer.accept(buffer, index, length, baseOffset, progressHandler)

            // TODO: eliminate iterator allocation
            for (PartitionResponseConsumer recordsConsumer : recordConsumers)
            {
                recordsConsumer.accept(buffer, index, length, progressHandler);
            }

            partitions.removeIf(p -> p.refs == 0);
        }

        int maximumWritableBytes()
        {
            // TODO: eliminate iterator allocation
            int writeableBytes = 0;
            for (IntSupplier supplyWindow : windowSuppliers)
            {
                writeableBytes = Math.max(writeableBytes, supplyWindow.getAsInt());
            }

            return writeableBytes;
        }

        private void handleProgress(
            int partitionId,
            long firstOffset,
            long nextOffset)
        {
            candidate.id = partitionId;
            candidate.offset = firstOffset;
            NetworkTopicPartition first = partitions.floor(candidate);
            assert first != null;
            assert first.offset == firstOffset;
            first.refs--;

            candidate.offset = nextOffset;
            NetworkTopicPartition next = partitions.floor(candidate);
            if (next == null || next.offset != nextOffset)
            {
                next = new NetworkTopicPartition();
                next.id = partitionId;
                next.offset = nextOffset;
                partitions.add(next);
            }
            next.refs++;
        }
    }

    static final class NetworkTopicPartition implements Comparable<NetworkTopicPartition>
    {
        int id;
        long offset;
        private int refs;

        @Override
        public int compareTo(
            NetworkTopicPartition that)
        {
            int comparison = this.id - that.id;

            if (comparison == 0)
            {
                comparison = (int)(((this.offset - that.offset) >> 32) & 0xFFFF_FFFF);
            }

            if (comparison == 0)
            {
                comparison = (int)((this.offset - that.offset) & 0xFFFF_FFFF);
            }

            return comparison;
        }

        @Override
        public String toString()
        {
            return String.format("id=%d, offset=%d, refs=%d", id, offset, refs);
        }
    }
}