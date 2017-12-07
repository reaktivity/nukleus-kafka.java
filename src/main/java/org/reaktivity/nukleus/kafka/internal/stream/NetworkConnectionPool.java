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
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.INVALID_TOPIC_EXCEPTION;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.LEADER_NOT_AVAILABLE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.NONE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.UNKNOWN_TOPIC_OR_PARTITION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.LongIterator;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.function.PartitionResponseConsumer;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
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
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.BrokerMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.MetadataRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.MetadataResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.MetadataResponsePart2FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.PartitionMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.TopicMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.specification.tcp.internal.types.stream.TcpBeginExFW;

final class NetworkConnectionPool
{
    private static final short FETCH_API_VERSION = 0x05;
    private static final short FETCH_API_KEY = 0x01;
    private static final short METADATA_API_VERSION = 0x05;
    private static final short METADATA_API_KEY = 0x03;

    private static final byte[] ANY_IP_ADDR = new byte[4];

    final RequestHeaderFW.Builder requestRW = new RequestHeaderFW.Builder();
    final FetchRequestFW.Builder fetchRequestRW = new FetchRequestFW.Builder();
    final TopicRequestFW.Builder topicRequestRW = new TopicRequestFW.Builder();
    final PartitionRequestFW.Builder partitionRequestRW = new PartitionRequestFW.Builder();
    final MetadataRequestFW.Builder metadataRequestRW = new MetadataRequestFW.Builder();

    final WindowFW windowRO = new WindowFW();
    final OctetsFW.Builder payloadRW = new OctetsFW.Builder();
    final TcpBeginExFW.Builder tcpBeginExRW = new TcpBeginExFW.Builder();

    final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    final TopicResponseFW topicResponseRO = new TopicResponseFW();
    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();

    final MetadataResponseFW metadataResponseRO = new MetadataResponseFW();
    final BrokerMetadataFW brokerMetadataRO = new BrokerMetadataFW();
    final MetadataResponsePart2FW metadataResponsePart2RO = new MetadataResponsePart2FW();
    final TopicMetadataFW topicMetadataRO = new TopicMetadataFW();
    final PartitionMetadataFW partitionMetadataRO = new PartitionMetadataFW();

    final MutableDirectBuffer encodeBuffer;

    private final ClientStreamFactory clientStreamFactory;
    private final String networkName;
    private final long networkRef;
    private final BufferPool bufferPool;
    private FetchConnection[] connections = new FetchConnection[0];
    private final MetadataConnection metadataConnection;

    private final Map<String, TopicMetadata> topicMetadataByName;
    private final Map<String, NetworkTopic> topicsByName;
    private final Int2ObjectHashMap<Consumer<Long2LongHashMap>> detachersById;

    private int nextAttachId;

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
        this.metadataConnection = new MetadataConnection();
        this.encodeBuffer = new UnsafeBuffer(new byte[clientStreamFactory.bufferPool.slotCapacity()]);
        this.topicsByName = new LinkedHashMap<>();
        this.topicMetadataByName = new HashMap<>();
        this.detachersById = new Int2ObjectHashMap<>();
    }

    void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        PartitionResponseConsumer consumeRecords,
        IntSupplier supplyWindow,
        IntConsumer newAttachIdConsumer,
        IntConsumer onMetadataError)
    {
        final TopicMetadata metadata = topicMetadataByName.computeIfAbsent(topicName, TopicMetadata::new);
        metadata.doAttach((m) ->
        {
            doAttach(topicName, fetchOffsets, consumeRecords, supplyWindow, newAttachIdConsumer, onMetadataError, m);
        });

        metadataConnection.doRequestIfNeeded();
    }

    private void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        PartitionResponseConsumer consumeRecords,
        IntSupplier supplyWindow,
        IntConsumer newAttachIdConsumer,
        IntConsumer onMetadataError,
        TopicMetadata topicMetadata)
    {
        short errorCode = topicMetadata.errorCode();
        switch(errorCode)
        {
        case UNKNOWN_TOPIC_OR_PARTITION:
            onMetadataError.accept(errorCode);
            break;
        case INVALID_TOPIC_EXCEPTION:
            onMetadataError.accept(errorCode);
            break;
        case LEADER_NOT_AVAILABLE:
            // recoverable
            break;
        case NONE:
            while(fetchOffsets.size() < topicMetadata.partitionCount())
            {
                fetchOffsets.put(fetchOffsets.size(), 0L);
            }
            final NetworkTopic topic = topicsByName.computeIfAbsent(topicName, NetworkTopic::new);
            topic.doAttach(fetchOffsets, consumeRecords, supplyWindow);

            final int newAttachId = nextAttachId++;

            detachersById.put(newAttachId, f -> topic.doDetach(f, consumeRecords, supplyWindow));

            topicMetadata.visitBrokers(broker ->
            {
                FetchConnection current = null;
                for (FetchConnection candidate : connections)
                {
                    if (candidate.brokerId == broker.nodeId)
                    {
                        current = candidate;
                        if (!current.host.equals(broker.host) || current.port != broker.port)
                        {
                            // Change in cluster configuration
                            current.close();
                            current = new FetchConnection(broker.nodeId, broker.host, broker.port);
                        }
                        break;
                    }
                }
                if (current == null)
                {
                    current = new FetchConnection(broker.nodeId, broker.host, broker.port);
                    connections = ArrayUtil.add(connections, current);
                }
            });
            newAttachIdConsumer.accept(newAttachId);
            break;
        default:
            break;
        }
    }

    void doFlush(
        long connectionPoolAttachId)
    {
        for (FetchConnection connection  : connections)
        {
            connection.doRequestIfNeeded();
        }
    }

    void doDetach(
        int attachId,
        Long2LongHashMap fetchOffsets)
    {
        final Consumer<Long2LongHashMap> detacher = detachersById.remove(attachId);
        if (detacher != null)
        {
            detacher.accept(fetchOffsets);
        }
        // TODO: If the topic now has no partitions, remove from topicsByName and topicMetadataByName
    }

    abstract class NetworkConnection
    {
        final MessageConsumer networkTarget;

        long networkId;

        int networkRequestBudget;
        int networkRequestPadding;

        int networkResponseBudget;
        int networkResponsePadding;

        MessageConsumer networkReplyThrottle;
        long networkReplyId;

        private MessageConsumer streamState;

        int networkSlot = NO_SLOT;
        int networkSlotOffset;

        int nextRequestId;
        int nextResponseId;

        private NetworkConnection()
        {
            this.networkTarget = NetworkConnectionPool.this.clientStreamFactory.router.supplyTarget(networkName);
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

        void doBeginIfNotConnected()
        {
            doBeginIfNotConnected((b, o, m) ->
            {
                return 0;
            });
        }

        void doBeginIfNotConnected(Flyweight.Builder.Visitor extensionVisitor)
        {
            if (networkId == 0L)
            {
                // TODO: progressive back-off before reconnect
                //       if choose to give up, say after maximum retry attempts,
                //       then send END to each consumer to clean up

                final long newNetworkId = NetworkConnectionPool.this.clientStreamFactory.supplyStreamId.getAsLong();
                final long newCorrelationId = NetworkConnectionPool.this.clientStreamFactory.supplyCorrelationId.getAsLong();

                NetworkConnectionPool.this.clientStreamFactory.correlations.put(newCorrelationId, NetworkConnection.this);

                NetworkConnectionPool.this.clientStreamFactory
                    .doBegin(networkTarget, newNetworkId, networkRef, newCorrelationId, extensionVisitor);
                NetworkConnectionPool.this.clientStreamFactory.router.setThrottle(
                        networkName, newNetworkId, this::handleThrottle);

                this.networkId = newNetworkId;
            }
        }

        abstract void doRequestIfNeeded();

        void close()
        {
            if (networkId != 0L)
            {
                NetworkConnectionPool.this.clientStreamFactory
                    .doEnd(networkTarget, networkId);
                this.networkId = 0L;
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

            doRequestIfNeeded();
        }

        private void handleReset(
            ResetFW reset)
        {
            doReinitialize();
            doRequestIfNeeded();
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
                        handleResponse(networkBuffer, networkOffset, networkLimit);
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
                        doRequestIfNeeded();
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

        abstract void handleResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            int networkLimit);

        private void handleEnd(
            EndFW end)
        {
            doReinitialize();
            doRequestIfNeeded();
        }

        private void handleAbort(
            AbortFW abort)
        {
            doReinitialize();
            doRequestIfNeeded();
        }

        void doOfferResponseBudget()
        {
            final int networkResponseCredit =
                    Math.max(NetworkConnectionPool.this.bufferPool.slotCapacity() - networkSlotOffset - networkResponseBudget, 0);

            NetworkConnectionPool.this.clientStreamFactory.doWindow(
                    networkReplyThrottle, networkReplyId, networkResponseCredit, networkResponsePadding);

            this.networkResponseBudget += networkResponseCredit;
        }

        private void doReinitialize()
        {
            networkRequestBudget = 0;
            networkRequestPadding = 0;
            networkId = 0L;
            nextRequestId = 0;
            nextResponseId = 0;
        }
    }

    final class FetchConnection extends NetworkConnection
    {
        private final int brokerId;
        private final String host;
        private final int port;

        private FetchConnection(int brokerId, String host, int port)
        {
            super();
            this.brokerId = brokerId;
            this.host = host;
            this.port = port;
        }

        @Override
        void doRequestIfNeeded()
        {
            if (nextRequestId == nextResponseId)
            {
                doBeginIfNotConnected((b, o, m) ->
                {
                    return tcpBeginExRW.wrap(b, o, m)
                            .localAddress(lab -> lab.ipv4Address(ob -> ob.put(ANY_IP_ADDR)))
                                      .localPort(0)
                                      .remoteAddress(rab -> rab.host(host))
                                      .remotePort(port)
                                      .limit();
                });

                if (networkRequestBudget > networkRequestPadding)
                {
                    final int encodeOffset = 512;
                    int encodeLimit = encodeOffset;
                    RequestHeaderFW request = requestRW.wrap(
                            NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                            NetworkConnectionPool.this.encodeBuffer.capacity())
                            .size(0)
                            .apiKey(FETCH_API_KEY)
                            .apiVersion(FETCH_API_VERSION)
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
                        final int[] nodeIdsByPartition = topicMetadataByName.get(topicName).nodeIdsByPartition;

                        maxFetchBytes += maxPartitionBytes;
                        int partitionCount = 0;
                        int partitionId = -1;
                        // TODO: eliminate iterator allocation
                        for (NetworkTopicPartition partition : topic.partitions)
                        {
                            if (partitionId < partition.id && nodeIdsByPartition[partition.id] == brokerId)
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
                                 .apiKey(FETCH_API_KEY)
                                 .apiVersion(FETCH_API_VERSION)
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

        @Override
        void handleResponse(
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

        @Override
        public String toString()
        {
            return String.format("[brokerId=%d, host=%s, port=%d, budget=%d, padding=%d]",
                    brokerId, host, port, networkRequestBudget, networkRequestPadding);
        }
    }

    private final class MetadataConnection extends NetworkConnection
    {
        TopicMetadata pendingTopicMetadata;

        @Override
        void doRequestIfNeeded()
        {
            Optional<TopicMetadata> topicMetadata;
            if (nextRequestId == nextResponseId && (topicMetadata = topicMetadataByName.values().stream()
                    .filter(m -> m.nodeIdsByPartition == null).findFirst()).isPresent())
            {
                pendingTopicMetadata = topicMetadata.get();
                doBeginIfNotConnected();

                if (networkRequestBudget > networkRequestPadding)
                {
                    final int encodeOffset = 0;
                    int encodeLimit = encodeOffset;

                    RequestHeaderFW request = requestRW.wrap(
                            NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                            NetworkConnectionPool.this.encodeBuffer.capacity())
                            .size(0)
                            .apiKey(METADATA_API_KEY)
                            .apiVersion(METADATA_API_VERSION)
                            .correlationId(0)
                            .clientId((String) null)
                            .build();

                    encodeLimit = request.limit();

                    MetadataRequestFW metadataRequest = metadataRequestRW.wrap(
                            NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                            NetworkConnectionPool.this.encodeBuffer.capacity())
                            .topicCount(1)
                            .topicName(topicMetadata.get().topicName)
                            .build();

                    encodeLimit = metadataRequest.limit();

                    // TODO: stream large requests in multiple DATA frames as needed
                    if (encodeLimit - encodeOffset + networkRequestPadding <= networkRequestBudget)
                    {
                        int newCorrelationId = nextRequestId++;

                        NetworkConnectionPool.this.requestRW
                                 .wrap(NetworkConnectionPool.this.encodeBuffer, request.offset(), request.limit())
                                 .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                                 .apiKey(METADATA_API_KEY)
                                 .apiVersion(METADATA_API_VERSION)
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

        @Override
        void handleResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
            final MetadataResponseFW metadataResponse =
                    NetworkConnectionPool.this.metadataResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
            final int brokerCount = metadataResponse.brokerCount();
            pendingTopicMetadata.initializeBrokers(brokerCount);
            networkOffset = metadataResponse.limit();
            for (int brokerIndex=0; brokerIndex < brokerCount; brokerIndex++)
            {
                final BrokerMetadataFW broker =
                        NetworkConnectionPool.this.brokerMetadataRO.wrap(networkBuffer, networkOffset, networkLimit);
                pendingTopicMetadata.addBroker(broker.nodeId(), broker.host().asString(), broker.port());
                networkOffset = broker.limit();
            }

            final MetadataResponsePart2FW metadataResponsePart2 =
                    NetworkConnectionPool.this.metadataResponsePart2RO.wrap(networkBuffer, networkOffset, networkLimit);
            final int topicCount = metadataResponsePart2.topicCount();
            assert topicCount == 1;
            networkOffset = metadataResponsePart2.limit();
            short errorCode = NONE;
            for (int topicIndex = 0; topicIndex < topicCount && errorCode == NONE; topicIndex++)
            {
                final TopicMetadataFW topicMetadata =
                        NetworkConnectionPool.this.topicMetadataRO.wrap(networkBuffer, networkOffset, networkLimit);
                final String topicName = topicMetadata.topic().asString();
                assert topicName.equals(pendingTopicMetadata.topicName);
                errorCode = topicMetadata.errorCode();
                if (errorCode != NONE)
                {
                    break;
                }
                final int partitionCount = topicMetadata.partitionCount();
                networkOffset = topicMetadata.limit();

                pendingTopicMetadata.initializePartitions(partitionCount);
                for (int partitionIndex = 0; partitionIndex < partitionCount && errorCode == NONE; partitionIndex++)
                {
                    final PartitionMetadataFW partition =
                            NetworkConnectionPool.this.partitionMetadataRO.wrap(networkBuffer, networkOffset, networkLimit);
                    errorCode = partition.errorCode();
                    pendingTopicMetadata.addPartition(partition.partitionId(), partition.leader());
                    networkOffset = partition.limit();
                }
            }
            if (KafkaErrors.isRecoverable(errorCode))
            {
                pendingTopicMetadata.reset();
                doRequestIfNeeded();
            }
            else
            {
                pendingTopicMetadata.setErrorCode(errorCode);
                pendingTopicMetadata.flush();
            }
        }
    }

    private final class NetworkTopic
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

    private static final class NetworkTopicPartition implements Comparable<NetworkTopicPartition>
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

    private static final class BrokerMetadata
    {
        final int nodeId;
        final String host;
        final int port;

        BrokerMetadata(int nodeId, String host, int port)
        {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

    }

    private static final class TopicMetadata
    {
        private final String topicName;
        private short errorCode;
        BrokerMetadata[] brokers;
        private int nextBrokerIndex;
        private int[] nodeIdsByPartition;
        private List<Consumer<TopicMetadata>> consumers = new ArrayList<>();

        TopicMetadata(String topicName)
        {
            this.topicName = topicName;
        }

        void setErrorCode(
            short errorCode)
        {
            this.errorCode = errorCode;
        }

        void initializeBrokers(int brokerCount)
        {
            brokers = new BrokerMetadata[brokerCount];
        }

        void addBroker(int nodeId, String host, int port)
        {
            brokers[nextBrokerIndex++] = new BrokerMetadata(nodeId, host, port);
        }

        void initializePartitions(int partitionCount)
        {
            nodeIdsByPartition = new int[partitionCount];
        }

        void addPartition(int partitionId, int nodeId)
        {
            nodeIdsByPartition[partitionId] = nodeId;
        }

        void doAttach(
            Consumer<TopicMetadata> consumer)
        {
            consumers.add(consumer);
            if (nodeIdsByPartition != null)
            {
                flush();
            }
        }

        void flush()
        {
            for (Consumer<TopicMetadata> consumer: consumers)
            {
                consumer.accept(this);
            }
            consumers.clear();
        }

        short errorCode()
        {
            return errorCode;
        }

        void visitBrokers(Consumer<BrokerMetadata> visitor)
        {
            for (BrokerMetadata broker : brokers)
            {
                visitor.accept(broker);
            }
        }

        int partitionCount()
        {
            return nodeIdsByPartition.length;
        }

        void reset()
        {
            brokers = null;
            nodeIdsByPartition = null;
            nextBrokerIndex = 0;
        }

    }
}