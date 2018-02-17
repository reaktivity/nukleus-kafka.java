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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.kafka.internal.stream.ClientStreamFactory.EMPTY_BYTE_ARRAY;
import static org.reaktivity.nukleus.kafka.internal.stream.ClientStreamFactory.REGION_METADATA_SIZE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.INVALID_TOPIC_EXCEPTION;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.LEADER_NOT_AVAILABLE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.NONE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.FIN;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.RST;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.isFin;
import static org.reaktivity.nukleus.kafka.internal.util.FrameFlags.isReset;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.LongIterator;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.function.IntBooleanConsumer;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.RequestHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.config.ConfigResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.config.DescribeConfigsRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.config.DescribeConfigsResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.config.ResourceRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.config.ResourceResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.BrokerMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.MetadataRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.MetadataResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.MetadataResponsePart2FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.PartitionMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.metadata.TopicMetadataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AckFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TransferFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;

final class NetworkConnectionPool
{
    private static final short FETCH_API_VERSION = 5;
    private static final short FETCH_API_KEY = 1;
    private static final short METADATA_API_VERSION = 5;
    private static final short METADATA_API_KEY = 3;
    private static final short DESCRIBE_CONFIGS_API_VERSION = 0;
    private static final short DESCRIBE_CONFIGS_API_KEY = 32;

    private static final byte RESOURCE_TYPE_TOPIC = 2;
    private static final String CLEANUP_POLICY = "cleanup.policy";
    private static final String COMPACT = "compact";

    private static final byte[] ANY_IP_ADDR = new byte[4];

    final RequestHeaderFW.Builder requestRW = new RequestHeaderFW.Builder();
    final FetchRequestFW.Builder fetchRequestRW = new FetchRequestFW.Builder();
    final TopicRequestFW.Builder topicRequestRW = new TopicRequestFW.Builder();
    final PartitionRequestFW.Builder partitionRequestRW = new PartitionRequestFW.Builder();
    final MetadataRequestFW.Builder metadataRequestRW = new MetadataRequestFW.Builder();
    final DescribeConfigsRequestFW.Builder describeConfigsRequestRW = new DescribeConfigsRequestFW.Builder();
    final ResourceRequestFW.Builder resourceRequestRW = new ResourceRequestFW.Builder();
    final String16FW.Builder configNameRW = new String16FW.Builder(ByteOrder.BIG_ENDIAN);


    final OctetsFW.Builder payloadRW = new OctetsFW.Builder();
    final TcpBeginExFW.Builder tcpBeginExRW = new TcpBeginExFW.Builder();
    final UnsafeBuffer bufferRW = new UnsafeBuffer(new byte[0]);

    final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    final TopicResponseFW topicResponseRO = new TopicResponseFW();
    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();

    final RecordBatchFW recordBatchRO = new RecordBatchFW();
    final RecordFW recordRO = new RecordFW();
    final UnsafeBuffer headersBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    final ListFW<HeaderFW> headersRO = new ListFW<>(new HeaderFW());
    final UnsafeBuffer keyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    final UnsafeBuffer regionBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    final MetadataResponseFW metadataResponseRO = new MetadataResponseFW();
    final BrokerMetadataFW brokerMetadataRO = new BrokerMetadataFW();
    final MetadataResponsePart2FW metadataResponsePart2RO = new MetadataResponsePart2FW();
    final TopicMetadataFW topicMetadataRO = new TopicMetadataFW();
    final PartitionMetadataFW partitionMetadataRO = new PartitionMetadataFW();
    final DescribeConfigsResponseFW describeConfigsResponseRO = new DescribeConfigsResponseFW();
    final ResourceResponseFW resourceResponseRO = new ResourceResponseFW();
    final ConfigResponseFW configResponseRO = new ConfigResponseFW();

    final MutableDirectBuffer encodeBuffer;
    final int maximumUnacknowledgedBytes;
    private final ClientStreamFactory clientStreamFactory;
    private final String networkName;
    private final long networkRef;
    private final MemoryManager memoryManager;
    private final int maximumBrokerReconnects;

    private AbstractFetchConnection[] connections = new LiveFetchConnection[0];
    private HistoricalFetchConnection[] historicalConnections = new HistoricalFetchConnection[0];
    private final MetadataConnection metadataConnection;

    private final Map<String, TopicMetadata> topicMetadataByName;
    private final Map<String, NetworkTopic> topicsByName;
    private final Int2ObjectHashMap<Consumer<Long2LongHashMap>> detachersById;
    private final int maximumFetchBytesLimit;

    private int nextAttachId;

    NetworkConnectionPool(
        ClientStreamFactory clientStreamFactory,
        String networkName,
        long networkRef,
        MemoryManager memoryManager)
    {
        this.clientStreamFactory = clientStreamFactory;
        this.networkName = networkName;
        this.networkRef = networkRef;
        this.memoryManager = memoryManager;
        this.metadataConnection = new MetadataConnection();
        this.maximumUnacknowledgedBytes = clientStreamFactory.configuration.maximumUnacknowledgedBytes();
        this.encodeBuffer = new UnsafeBuffer(new byte[this.maximumUnacknowledgedBytes]);
        this.topicsByName = new LinkedHashMap<>();
        this.topicMetadataByName = new HashMap<>();
        this.detachersById = new Int2ObjectHashMap<>();
        this.maximumBrokerReconnects = clientStreamFactory.configuration.maximumBrokerReconnects();
        int maximumMessageSize = clientStreamFactory.configuration.maximumMessageSize();
        this.maximumFetchBytesLimit = maximumUnacknowledgedBytes > maximumMessageSize ?
                maximumUnacknowledgedBytes - maximumMessageSize : maximumUnacknowledgedBytes;
    }

    void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        int partitionHash,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        IntBooleanConsumer newAttachDetailsConsumer,
        IntConsumer onMetadataError)
    {
        final TopicMetadata metadata = topicMetadataByName.computeIfAbsent(topicName, TopicMetadata::new);
        metadata.doAttach((m) ->
        {
            doAttach(topicName, fetchOffsets, partitionHash,  fetchKey, headers, dispatcher, progressHandlerConsumer,
                    newAttachDetailsConsumer, onMetadataError, m);
        });

        metadataConnection.doRequestIfNeeded();
    }

    private void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        int partitionHash,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        IntBooleanConsumer newAttachDetailsConsumer,
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
            if (fetchKey != null)
            {
                int partitionId = BufferUtil.partition(partitionHash, topicMetadata.partitionCount());
                fetchOffsets.computeIfAbsent(0L, v -> 0L);
                if (partitionId != 0)
                {
                    long offset = fetchOffsets.remove(0L);
                    fetchOffsets.put(partitionId, offset);
                }
            }
            else
            {
                while(fetchOffsets.size() < topicMetadata.partitionCount())
                {
                    fetchOffsets.put(fetchOffsets.size(), 0L);
                }
            }
            finishAttach(topicName, fetchOffsets, fetchKey, headers, dispatcher, progressHandlerConsumer,
                    newAttachDetailsConsumer, topicMetadata);
            break;
        default:
            throw new RuntimeException(format("Unexpected errorCode %d from metadata query", errorCode));
        }
    }

    private void finishAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        IntBooleanConsumer newAttachDetailsConsumer,
        TopicMetadata topicMetadata)
    {
        final NetworkTopic topic = topicsByName.computeIfAbsent(topicName,
                name -> new NetworkTopic(name, topicMetadata.isCompacted()));
        progressHandlerConsumer.accept(topic.doAttach(fetchOffsets, fetchKey, headers, dispatcher));

        final int newAttachId = nextAttachId++;

        detachersById.put(newAttachId, f -> topic.doDetach(f, fetchKey, headers, dispatcher));

        topicMetadata.visitBrokers(broker ->
        {
            connections = applyBrokerMetadata(connections, broker, LiveFetchConnection::new);
        });
        topicMetadata.visitBrokers(broker ->
        {
            historicalConnections = applyBrokerMetadata(historicalConnections, broker,  HistoricalFetchConnection::new);
        });
        newAttachDetailsConsumer.accept(newAttachId, topicMetadata.compacted);
        doFlush();
    }

    private <T extends AbstractFetchConnection> T[] applyBrokerMetadata(
        T[] connections,
        BrokerMetadata broker,
        Function<BrokerMetadata, T> createConnection)
    {
        T[] result = connections;
        AbstractFetchConnection current = null;
        for (int i = 0; i < connections.length; i++)
        {
            AbstractFetchConnection connection = connections[i];
            if (connection.brokerId == broker.nodeId)
            {
                current = connection;
                if (!connection.host.equals(broker.host) || connection.port != broker.port)
                {
                    // Change in cluster configuration
                    connection.close();
                    current = createConnection.apply(broker);
                }
                break;
            }
        }
        if (current == null)
        {
            result = ArrayUtil.add(connections, createConnection.apply(broker));
        }
        return result;
    }

    void doFlush()
    {
        for (AbstractFetchConnection connection  : connections)
        {
            connection.doRequestIfNeeded();
        }
        for (AbstractFetchConnection connection  : historicalConnections)
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
    }

    abstract class AbstractNetworkConnection
    {
        final MessageConsumer networkTarget;

        long networkId;

        MessageConsumer networkReplyThrottle;
        long networkReplyId;

        private MessageConsumer streamState;

        long encodeAddress = -1L;
        int encodeLength;
        int reconnects;

        int nextRequestId;
        int nextResponseId;

        long requestRegionAddress;
        int requestRegionCapacity;
        int requestUnacknowledgedBytes;

        DirectBuffer responseBuffer = null;
        ListFW<RegionFW> responseRegions = null;
        final DirectBufferBuilder directBufferBuilder;

        private AbstractNetworkConnection()
        {
            this.networkTarget = NetworkConnectionPool.this.clientStreamFactory.router.supplyTarget(networkName);
            directBufferBuilder = clientStreamFactory.supplyDirectBufferBuilder.get();
        }

        @Override
        public String toString()
        {
            return String.format("[nextRequestId=%d, nextResponseId=%d]", nextRequestId, nextResponseId);
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

                NetworkConnectionPool.this.clientStreamFactory.correlations.put(newCorrelationId, AbstractNetworkConnection.this);

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
                    .doTransfer(networkTarget, networkId, FIN);
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
            case AckFW.TYPE_ID:
                final AckFW ack = clientStreamFactory.ackRO.wrap(buffer, index, index + length);
                handleAck(ack);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleAck(
            final AckFW ack)
        {
            final ListFW<RegionFW> regions = ack.regions();
            if (!regions.isEmpty())
            {
                regions.forEach(r -> requestUnacknowledgedBytes  -= r.length());
                if (requestUnacknowledgedBytes == 0)
                {
                     memoryManager.release(requestRegionAddress, requestRegionCapacity);
                }
            }
            if (isReset(ack.flags()))
            {
                if (networkReplyId == 0L)
                {
                    NetworkConnectionPool.this.clientStreamFactory.correlations.remove(networkId, AbstractNetworkConnection.this);
                }
                else
                {
                    doAck(RST, responseRegions);
                }
                reconnectAsConfigured();
            }
            else
            {
                doRequestIfNeeded();
            }
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
                NetworkConnectionPool.this.clientStreamFactory.doAckReset(networkReplyThrottle, networkReplyId);
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
            case TransferFW.TYPE_ID:
                final TransferFW transfer = NetworkConnectionPool.this.clientStreamFactory.transferRO.wrap(
                        buffer, index, index + length);
                handleTransfer(transfer);
                break;
            default:
                NetworkConnectionPool.this.clientStreamFactory.doAckReset(networkReplyThrottle, networkReplyId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            this.streamState = this::afterBegin;
            doAck(0, null);
        }

        private void handleTransfer(
            TransferFW transfer)
        {
            ListFW<RegionFW> regions = transfer.regions();
            if (responseBuffer != null)
            {
                directBufferBuilder.wrap(responseBuffer);
            }
            regions.forEach(r -> directBufferBuilder.wrap(memoryManager.resolve(r.address()), r.length()));
            DirectBuffer networkBuffer = directBufferBuilder.build();
            int networkOffset = 0;
            int networkLimit = networkBuffer.capacity();
            try
            {
                ResponseHeaderFW response = null;
                if (networkOffset + BitUtil.SIZE_OF_INT <= networkLimit)
                {
                    response = NetworkConnectionPool.this.responseRO.wrap(networkBuffer, networkOffset, networkLimit);
                    networkOffset = response.limit();
                }

                if (response == null || networkOffset + response.size() > networkLimit)
                {
                    if (networkBuffer.capacity() >= maximumUnacknowledgedBytes)
                    {
                        // response size exceeds maximum we can currently handle
                        throw new IllegalStateException(format("%s: Kafka response size %d exceeds %d bytes",
                                this, response.size(), maximumUnacknowledgedBytes));
                    }
                    responseRegions = appendRegions(responseRegions, regions);
                    responseBuffer = networkBuffer;
                }
                else
                {
                    handleResponse(networkBuffer, networkOffset, networkLimit);
                    responseBuffer = null;
                    doAck(0, responseRegions);
                    responseRegions = null;
                    networkOffset += response.size();
                    nextResponseId++;
                    assert networkOffset == networkLimit;
                    doRequestIfNeeded();
                }
            }
            finally
            {
                if (transfer.flags() != 0)
                {
                    if (isReset(transfer.flags()))
                    {
                        doAck(RST, responseRegions);
                    }
                    else if (isFin(transfer.flags()))
                    {
                        doAck(FIN, responseRegions);
                    }
                    reconnectAsConfigured();
                }
            }
        }

        private ListFW<RegionFW> appendRegions(ListFW<RegionFW> regions1, ListFW<RegionFW> regions2)
        {
            ListFW<RegionFW> result = regions1;
            int capacity = regions1 == null ? regions2 == null ? 0 : regions2.sizeof() :
                regions1.sizeof() + regions2.sizeof() - Integer.BYTES;
            if (capacity > 0)
            {
                UnsafeBuffer buffer = new UnsafeBuffer(new byte[capacity]);
                ListFW.Builder<RegionFW.Builder, RegionFW>builder = new ListFW.Builder<>(new RegionFW.Builder(), new RegionFW())
                        .wrap(buffer, 0, buffer.capacity());
                if (regions1 != null)
                {
                    regions1.forEach(r -> builder.item(b -> b.address(r.address()).length(r.length()).streamId(r.streamId())));
                }
                regions2.forEach(r -> builder.item(b -> b.address(r.address()).length(r.length()).streamId(r.streamId())));
                result = builder.build();
            }
            return result;
        }

        abstract void handleResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            int networkLimit);

        void doAck(int flags, ListFW<RegionFW> regions)
        {
            NetworkConnectionPool.this.clientStreamFactory.doAck(
                    networkReplyThrottle, networkReplyId, flags, regions);
        }

        private void reconnectAsConfigured()
        {
            if (reconnects < maximumBrokerReconnects)
            {
                reconnects++;
                networkId = 0L;
                networkReplyId = 0L;
                nextRequestId = 0;
                nextResponseId = 0;
                responseBuffer = null;
                responseRegions = null;
                requestUnacknowledgedBytes = 0;
                doRequestIfNeeded();
            }
        }
    }

    abstract class AbstractFetchConnection extends AbstractNetworkConnection
    {
        private final String host;
        private final int port;

        final int brokerId;
        int encodeLimit;

        private AbstractFetchConnection(BrokerMetadata broker)
        {
            super();
            this.brokerId = broker.nodeId;
            this.host = broker.host;
            this.port = broker.port;
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

                if (requestUnacknowledgedBytes == 0)
                {
                    final int encodeOffset = 0;
                    int topicCount = 0;

                    encodeLimit = encodeOffset;
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

                    // TODO: stream large requests in multiple Transfer frames as needed
                    for (String topicName : topicsByName.keySet())
                    {
                        final int originalEncodeLimit = encodeLimit;
                        TopicRequestFW topicRequest = topicRequestRW.wrap(
                                NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                NetworkConnectionPool.this.encodeBuffer.capacity())
                                .name(topicName)
                                .partitionCount(0)
                                .build();

                        encodeLimit = topicRequest.limit();
                        int partitionCount = addTopicToRequest(topicName);

                        if (partitionCount > 0)
                        {
                            NetworkConnectionPool.this.topicRequestRW
                                  .wrap(NetworkConnectionPool.this.encodeBuffer, topicRequest.offset(), topicRequest.limit())
                                  .name(topicRequest.name())
                                  .partitionCount(partitionCount)
                                  .build();
                            topicCount++;
                        }
                        else
                        {
                            encodeLimit = originalEncodeLimit;
                        }
                    }

                    if (topicCount > 0)
                    {
                        NetworkConnectionPool.this.fetchRequestRW
                        .wrap(NetworkConnectionPool.this.encodeBuffer, fetchRequest.offset(), fetchRequest.limit())
                        .maxWaitTimeMillis(fetchRequest.maxWaitTimeMillis())
                        .minBytes(fetchRequest.minBytes())
                        .maxBytes(maximumFetchBytesLimit)
                        .isolationLevel(fetchRequest.isolationLevel())
                        .topicCount(topicCount)
                        .build();

                        int newCorrelationId = nextRequestId++;
                        NetworkConnectionPool.this.requestRW
                                 .wrap(NetworkConnectionPool.this.encodeBuffer, request.offset(), request.limit())
                                 .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                                 .apiKey(FETCH_API_KEY)
                                 .apiVersion(FETCH_API_VERSION)
                                 .correlationId(newCorrelationId)
                                 .clientId((String) null)
                                 .build();

                        requestRegionCapacity = encodeLimit - encodeOffset;
                        requestRegionAddress = memoryManager.acquire(requestRegionCapacity);
                        regionBuffer.wrap(memoryManager.resolve(requestRegionAddress), requestRegionCapacity);
                        regionBuffer.putBytes(0, encodeBuffer, 0, requestRegionCapacity);

                        NetworkConnectionPool.this.clientStreamFactory.doTransfer(networkTarget, networkId, 0,
                                b -> b.item(r -> r.address(requestRegionAddress)
                                                  .length(requestRegionCapacity)
                                                  .streamId(networkId)));
                        requestUnacknowledgedBytes = requestRegionCapacity;
                    }
                }
            }
        }

        abstract int addTopicToRequest(
            String topicName);

        @Override
        final void handleResponse(
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
                                                  partitionResponseSize,
                                                  getRequestedOffset(topic, partitionResponse.partitionId()));
                    }
                }
            }
        }

        abstract long getRequestedOffset(NetworkTopic topic, int partitionId);

        @Override
        public String toString()
        {
            return String.format("%s [brokerId=%d, host=%s, port=%d, maxFetchBytes=%d]",
                    getClass().getName(), brokerId, host, port, maximumFetchBytesLimit);
        }
    }

    private final class LiveFetchConnection extends AbstractFetchConnection
    {
        LiveFetchConnection(BrokerMetadata broker)
        {
            super(broker);
        }

        @Override
        int addTopicToRequest(
            String topicName)
        {
            NetworkTopic topic = topicsByName.get(topicName);
            final int maxPartitionBytes =  maximumFetchBytesLimit;
            final int[] nodeIdsByPartition = topicMetadataByName.get(topicName).nodeIdsByPartition;

            int partitionCount = 0;
            // TODO: eliminate iterator allocation
            Iterator<NetworkTopicPartition> iterator = topic.partitions.iterator();
            NetworkTopicPartition candidate = iterator.hasNext() ? iterator.next() : null;
            NetworkTopicPartition next;
            while (candidate != null)
            {
                next = iterator.hasNext() ? iterator.next() : null;
                boolean isHighestOffset = next == null || next.id != candidate.id;
                if (isHighestOffset && nodeIdsByPartition[candidate.id] == brokerId)
                {
                    PartitionRequestFW partitionRequest = NetworkConnectionPool.this.partitionRequestRW
                        .wrap(NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                NetworkConnectionPool.this.encodeBuffer.capacity())
                        .partitionId(candidate.id)
                        .fetchOffset(candidate.offset)
                        .maxBytes(maxPartitionBytes)
                        .build();

                    encodeLimit = partitionRequest.limit();
                    partitionCount++;
                }
                candidate = next;
            }
            return partitionCount;
        }

        @Override
        long getRequestedOffset(NetworkTopic topic, int partitionId)
        {
            return topic.getHighestOffset(partitionId);
        }
    }

    private final class HistoricalFetchConnection extends AbstractFetchConnection
    {
        private HistoricalFetchConnection(BrokerMetadata broker)
        {
            super(broker);
        }

        @Override
        int addTopicToRequest(
            String topicName)
        {
            int partitionCount = 0;
            NetworkTopic topic = topicsByName.get(topicName);
            if (topic.needsHistorical())
            {
                int maxPartitionBytes = maximumFetchBytesLimit;
                final int[] nodeIdsByPartition = topicMetadataByName.get(topicName).nodeIdsByPartition;

                int partitionId = -1;
                // TODO: eliminate iterator allocation
                for (NetworkTopicPartition partition : topic.partitions)
                {
                    if (topic.needsHistorical(partition.id) &&
                            partitionId < partition.id && nodeIdsByPartition[partition.id] == brokerId)
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
            }
            return partitionCount;
        }

        @Override
        long getRequestedOffset(NetworkTopic topic, int partitionId)
        {
            return topic.getLowestOffset(partitionId);
        }

    }

    private final class MetadataConnection extends AbstractNetworkConnection
    {
        TopicMetadata pendingTopicMetadata;
        boolean awaitingDescribeConfigs;

        @Override
        void doRequestIfNeeded()
        {
            Optional<TopicMetadata> topicMetadata;
            if (nextRequestId == nextResponseId)
            {
                if (pendingTopicMetadata == null &&
                    ((topicMetadata = topicMetadataByName.values().stream()
                      .filter(m -> m.nodeIdsByPartition == null).findFirst()).isPresent()))
                {
                    pendingTopicMetadata = topicMetadata.get();
                }
                if (pendingTopicMetadata != null)
                {
                    if (pendingTopicMetadata.hasMetadata())
                    {
                        doDescribeConfigsRequest();
                    }
                    else
                    {
                        doMetadataRequest();
                    }
                }
            }
        }

        private void doDescribeConfigsRequest()
        {
            doBeginIfNotConnected();
            if (requestUnacknowledgedBytes == 0)
            {
                final int encodeOffset = 0;
                final MutableDirectBuffer buffer = NetworkConnectionPool.this.encodeBuffer;
                int encodeLimit = encodeOffset;

                RequestHeaderFW request = requestRW.wrap(
                        buffer, encodeLimit, buffer.capacity())
                        .size(0)
                        .apiKey(DESCRIBE_CONFIGS_API_KEY)
                        .apiVersion(DESCRIBE_CONFIGS_API_VERSION)
                        .correlationId(0)
                        .clientId((String) null)
                        .build();

                encodeLimit = request.limit();

                DescribeConfigsRequestFW describeRequest = describeConfigsRequestRW.wrap(
                        buffer, encodeLimit, buffer.capacity())
                        .resourceCount(1)
                        .build();

                encodeLimit = describeRequest.limit();

                ResourceRequestFW resourceRequest = resourceRequestRW.wrap(
                        buffer, encodeLimit, buffer.capacity())
                        .type(RESOURCE_TYPE_TOPIC)
                        .name(pendingTopicMetadata.topicName)
                        .configNamesCount(1)
                        .build();

                encodeLimit = resourceRequest.limit();

                String16FW configName = configNameRW.wrap(
                        buffer, encodeLimit, buffer.capacity())
                        .set(CLEANUP_POLICY, UTF_8)
                        .build();

                encodeLimit = configName.limit();

                // TODO: stream large requests in multiple DATA frames as needed
                int newCorrelationId = nextRequestId++;

                NetworkConnectionPool.this.requestRW
                         .wrap(buffer, request.offset(), request.limit())
                         .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                         .apiKey(DESCRIBE_CONFIGS_API_KEY)
                         .apiVersion(DESCRIBE_CONFIGS_API_VERSION)
                         .correlationId(newCorrelationId)
                         .clientId((String) null)
                         .build();

                requestRegionCapacity = encodeLimit - encodeOffset;
                requestRegionAddress = memoryManager.acquire(requestRegionCapacity);
                regionBuffer.wrap(memoryManager.resolve(requestRegionAddress), requestRegionCapacity);
                regionBuffer.putBytes(0, encodeBuffer, 0, requestRegionCapacity);

                NetworkConnectionPool.this.clientStreamFactory.doTransfer(networkTarget, networkId, 0,
                        b -> b.item(r -> r.address(requestRegionAddress)
                                          .length(requestRegionCapacity)
                                          .streamId(networkId)));
                requestUnacknowledgedBytes = requestRegionCapacity;

                awaitingDescribeConfigs = true;
            }
        }

        private void doMetadataRequest()
        {
            doBeginIfNotConnected();

            if (requestUnacknowledgedBytes == 0)
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
                        .topicName(pendingTopicMetadata.topicName)
                        .build();

                encodeLimit = metadataRequest.limit();

                // TODO: stream large requests in multiple DATA frames as needed
                int newCorrelationId = nextRequestId++;

                NetworkConnectionPool.this.requestRW
                         .wrap(NetworkConnectionPool.this.encodeBuffer, request.offset(), request.limit())
                         .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                         .apiKey(METADATA_API_KEY)
                         .apiVersion(METADATA_API_VERSION)
                         .correlationId(newCorrelationId)
                         .clientId((String) null)
                         .build();

                requestRegionCapacity = encodeLimit - encodeOffset;
                requestRegionAddress = memoryManager.acquire(requestRegionCapacity);
                regionBuffer.wrap(memoryManager.resolve(requestRegionAddress), requestRegionCapacity);
                regionBuffer.putBytes(0, encodeBuffer, 0, requestRegionCapacity);

                NetworkConnectionPool.this.clientStreamFactory.doTransfer(networkTarget, networkId, 0,
                        b -> b.item(r -> r.address(requestRegionAddress)
                                          .length(requestRegionCapacity)
                                          .streamId(networkId)));
                requestUnacknowledgedBytes = requestRegionCapacity;
            }
        }

        @Override
        void handleResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
            if (awaitingDescribeConfigs)
            {
                handleDescribeConfigsResponse(networkBuffer, networkOffset, networkLimit);
            }
            else
            {
                handleMetadataResponse(networkBuffer, networkOffset, networkLimit);
            }
        }

        void handleDescribeConfigsResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
            awaitingDescribeConfigs = false;
            final DescribeConfigsResponseFW describeConfigsResponse =
                    NetworkConnectionPool.this.describeConfigsResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
            int resourceCount = describeConfigsResponse.resourceCount();
            networkOffset = describeConfigsResponse.limit();

            short errorCode = NONE;
            boolean compacted = false;
            for (int resourceIndex = 0; resourceIndex < resourceCount && errorCode == NONE; resourceIndex++)
            {
                final ResourceResponseFW resource =
                        NetworkConnectionPool.this.resourceResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                final String16FW name = resource.name();
                assert name.asString().equals(pendingTopicMetadata.topicName);
                errorCode = resource.errorCode();
                if (errorCode != NONE)
                {
                    break;
                }
                final int configCount = resource.configCount();
                networkOffset = resource.limit();

                for (int configIndex = 0; configIndex < configCount && errorCode == NONE; configIndex++)
                {
                    final ConfigResponseFW config =
                            NetworkConnectionPool.this.configResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                    String16FW configName = config.name();
                    assert configName.asString().equals(CLEANUP_POLICY);
                    compacted = config.value() != null && config.value().asString().equals(COMPACT);
                    networkOffset = config.limit();
                }
            }
            TopicMetadata metadata = pendingTopicMetadata;
            pendingTopicMetadata = null;
            metadata.setCompacted(compacted);
            metadata.setErrorCode(errorCode);
            metadata.flush();
        }

        void handleMetadataResponse(
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
                final String16FW topicName = topicMetadata.topic();
                assert topicName.asString().equals(pendingTopicMetadata.topicName);
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
            final TopicMetadata topicMetadata = pendingTopicMetadata;
            if (errorCode != NONE)
            {
                if (KafkaErrors.isRecoverable(errorCode))
                {
                    pendingTopicMetadata.reset();
                    doRequestIfNeeded();
                }
                else
                {
                    topicMetadataByName.remove(topicMetadata.topicName);
                    pendingTopicMetadata = null;
                    topicMetadata.setErrorCode(errorCode);
                    topicMetadata.flush();
                }
            }
        }
    }

    private final class NetworkTopic
    {
        private final String topicName;
        final NavigableSet<NetworkTopicPartition> partitions;
        private final NetworkTopicPartition candidate;
        private final TopicMessageDispatcher dispatcher = new TopicMessageDispatcher();
        private final PartitionProgressHandler progressHandler;
        private ListFW<HeaderFW> headers;
        private UnsafeBuffer header = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private UnsafeBuffer value1 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private UnsafeBuffer value2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

        private BitSet needsHistoricalByPartition = new BitSet();


        @Override
        public String toString()
        {
            return String.format("topicName=%s, partitions=%s", topicName, partitions);
        }

        NetworkTopic(
            String topicName, boolean compacted)
        {
            this.topicName = topicName;
            this.partitions = new TreeSet<>();
            this.candidate = new NetworkTopicPartition();
            this.progressHandler = this::handleProgress;
        }

        PartitionProgressHandler doAttach(
            Long2LongHashMap fetchOffsets,
            OctetsFW fetchKey,
            ListFW<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher)
        {
            this.dispatcher.add(fetchKey, headers, dispatcher);

            final LongIterator keys = fetchOffsets.keySet().iterator();
            while (keys.hasNext())
            {
                final long partitionId = (int) keys.nextValue();
                attachToPartition((int) partitionId, fetchOffsets.get(partitionId));
            }
             return progressHandler;
        }

        private void attachToPartition(
            int partitionId,
            long fetchOffset)
        {
            candidate.id = partitionId;
            candidate.offset = fetchOffset;
            NetworkTopicPartition partition = partitions.floor(candidate);
            if (partition == null || partition.id != candidate.id)
            {
                NetworkTopicPartition ceiling = partitions.ceiling(candidate);
                boolean needsHistorical = ceiling != null && ceiling.id == candidate.id;
                needsHistoricalByPartition.set(candidate.id, needsHistorical);
                partition = new NetworkTopicPartition();
                partition.id = candidate.id;
                partition.offset = candidate.offset;

                partitions.add(partition);
            }
            partition.refs++;
        }

        void doDetach(
            Long2LongHashMap fetchOffsets,
            OctetsFW fetchKey,
            ListFW<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher)
        {
            this.dispatcher.remove(fetchKey, headers, dispatcher);
            final LongIterator partitionIds = fetchOffsets.keySet().iterator();
            while (partitionIds.hasNext())
            {
                long partitionId = partitionIds.nextValue();
                doDetach((int) partitionId, fetchOffsets.get(partitionId));
            }
            if (partitions.isEmpty())
            {
                topicsByName.remove(topicName);
                topicMetadataByName.remove(topicName);
            }
        }

        void doDetach(
            int partitionId,
            long fetchOffset)
        {
            candidate.id = partitionId;
            candidate.offset = fetchOffset;
            NetworkTopicPartition partition = partitions.floor(candidate);
            if (partition != null)
            {
                assert partition.id == candidate.id;
                partition.refs--;

                if (partition.refs == 0)
                {
                    remove(partition);
                }
            }
        }

        long getHighestOffset(
            int partitionId)
        {
            candidate.id = partitionId;
            candidate.offset = Long.MAX_VALUE;
            NetworkTopicPartition floor = partitions.floor(candidate);
            assert floor == null || floor.id == partitionId;
            return floor != null ? floor.offset : 0L;
        }

        long getLowestOffset(
            int partitionId)
        {
            candidate.id = partitionId;
            candidate.offset = 0L;
            NetworkTopicPartition ceiling = partitions.ceiling(candidate);
            assert ceiling == null || ceiling.id == partitionId;
            return ceiling != null ? ceiling.offset : Long.MAX_VALUE;
        }

        void onPartitionResponse(
            DirectBuffer buffer,
            int offset,
            int length,
            long requestedOffset)
        {
            final int maxLimit = offset + length;
            int networkOffset = offset;
            final PartitionResponseFW partition = partitionResponseRO.wrap(buffer, networkOffset, maxLimit);
            networkOffset = partition.limit();
            final int partitionId = partition.partitionId();

            // TODO: determine appropriate reaction to different non-zero error codes
            if (partition.errorCode() == 0 && networkOffset < maxLimit - BitUtil.SIZE_OF_INT)
            {
                final RecordSetFW recordSet = recordSetRO.wrap(buffer, networkOffset, maxLimit);
                networkOffset = recordSet.limit();

                final int recordSetLimit = networkOffset + recordSet.recordBatchSize();
                if (recordSetLimit <= maxLimit)
                {
                    loop:
                    while (networkOffset < recordSetLimit - RecordBatchFW.FIELD_OFFSET_RECORD_COUNT - BitUtil.SIZE_OF_INT)
                    {
                        final RecordBatchFW recordBatch = recordBatchRO.wrap(buffer, networkOffset, recordSetLimit);
                        networkOffset = recordBatch.limit();

                        final int recordBatchLimit = recordBatch.offset() +
                                RecordBatchFW.FIELD_OFFSET_LENGTH + BitUtil.SIZE_OF_INT + recordBatch.length();
                        if (recordBatchLimit > recordSetLimit)
                        {
                            break loop;
                        }

                        final long firstOffset = recordBatch.firstOffset();
                        long nextFetchAt = firstOffset;
                        while (networkOffset < recordBatchLimit - 7 /* minimum RecordFW size */)
                        {
                            final RecordFW record = recordRO.wrap(buffer, networkOffset, recordBatchLimit);
                            networkOffset = record.limit();
                            final int recordLimit = record.offset() +
                                    RecordFW.FIELD_OFFSET_ATTRIBUTES + record.length();
                            if (recordLimit > recordSetLimit)
                            {
                                break loop;
                            }

                            final int headerCount = record.headerCount();
                            long headersAddress = 0;
                            int headersCapacity = 0;
                            if (headerCount > 0)
                            {
                                int headersOffset = networkOffset;
                                int headersSize = 0;
                                for (int headerIndex = 0; headerIndex < headerCount; headerIndex++)
                                {
                                    final HeaderFW headerRO = new HeaderFW();
                                    final HeaderFW header = headerRO.wrap(buffer, networkOffset, recordBatchLimit);
                                    networkOffset = header.limit();
                                    headersSize += header.sizeof();
                                }
                                headersCapacity = headersSize + Integer.BYTES;
                                headersAddress = memoryManager.acquire(headersCapacity);
                                headersBuffer.wrap(memoryManager.resolve(headersAddress), headersCapacity);
                                headersBuffer.putInt(0,  headersSize);
                                headersBuffer.putBytes(Integer.BYTES, buffer, headersOffset, headersSize);
                                headers = headersRO.wrap(headersBuffer, 0, headersBuffer.capacity());
                            }
                            else
                            {
                                headers = null;
                            }

                            final long currentFetchAt = firstOffset + record.offsetDelta();

                            nextFetchAt = currentFetchAt + 1;

                            DirectBuffer key = null;
                            final OctetsFW messageKey = record.key();
                            if (messageKey != null)
                            {
                                keyBuffer.wrap(messageKey.buffer(), messageKey.offset(), messageKey.sizeof());
                                keyBuffer.putBytes(0, messageKey.buffer(), messageKey.offset(), messageKey.sizeof());
                                key = keyBuffer;
                            }

                            final OctetsFW value = record.value();
                            if (value != null)
                            {
                                int regionSize = value.sizeof() + REGION_METADATA_SIZE;
                                long regionAddress = memoryManager.acquire(regionSize);
                                regionBuffer.wrap(memoryManager.resolve(regionAddress), value.sizeof());
                                regionBuffer.putBytes(0,  value.buffer(), value.offset(), value.sizeof());
                                int referenceCount = dispatcher.dispatch(partitionId, requestedOffset, nextFetchAt,
                                        key, this::supplyHeader, regionBuffer);
                                if (referenceCount == 0)
                                {
                                    memoryManager.release(regionAddress, regionSize);
                                }
                                else
                                {
                                    regionBuffer.wrap(memoryManager.resolve(regionAddress) + value.sizeof(),
                                            REGION_METADATA_SIZE);
                                    clientStreamFactory.regionMetadataRW.wrap(regionBuffer,  0,  regionBuffer.capacity())
                                        .endAdress(regionAddress + value.sizeof())  // end of region marker
                                        .regionCapacity(regionSize)
                                        .refCount(referenceCount)
                                        .build();
                                }
                            }
                            else
                            {
                                dispatcher.dispatch(partitionId, requestedOffset, nextFetchAt,
                                        key, this::supplyHeader, null);
                            }
                            if (headers != null)
                            {
                                memoryManager.release(headersAddress, headersCapacity);
                            }
                        }
                        dispatcher.flush(partitionId, requestedOffset, nextFetchAt);
                    }
                }
            }
        }

        private DirectBuffer supplyHeader(DirectBuffer headerName)
        {
            DirectBuffer result = null;
            if (headers != null)
            {
                HeaderFW matching = headers.matchFirst(h -> matches(h.key(), headerName));
                if (matching != null)
                {
                    OctetsFW value = matching.value();
                    header.wrap(value.buffer(), value.offset(), value.sizeof());
                    result = header;
                }
            }
            return result;
        }

        private boolean matches(OctetsFW octets, DirectBuffer buffer)
        {
            value1.wrap(octets.buffer(), octets.offset(), octets.sizeof());
            value2.wrap(buffer);
            return value1.equals(value2);
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
                if (next !=  null && next != first)
                {
                    needsHistoricalByPartition.set(partitionId);
                }
                next = new NetworkTopicPartition();
                next.id = partitionId;
                next.offset = nextOffset;
                partitions.add(next);
            }
            next.refs++;
            if (first.refs == 0)
            {
                remove(first);
            }
        }

        private void remove(
            NetworkTopicPartition partition)
        {
            partitions.remove(partition);
            boolean needsHistorical = false;
            partition.offset = 0;
            NetworkTopicPartition lowest = partitions.ceiling(partition);
            if (lowest != null && lowest.id == partition.id)
            {
                partition.offset = Long.MAX_VALUE;
                NetworkTopicPartition highest = partitions.floor(partition);
                // TODO: BUG must add && highest.id == partition.id
                if (highest != lowest)
                {
                    needsHistorical = true;
                }
            }
            needsHistoricalByPartition.set(partition.id, needsHistorical);
        }

        boolean needsHistorical()
        {
            return needsHistoricalByPartition.nextSetBit(0) != -1;
        }

        boolean needsHistorical(int partition)
        {
            return needsHistoricalByPartition.get(partition);
        }
    }

    private static final class NetworkTopicPartition implements Comparable<NetworkTopicPartition>
    {
        private static final NetworkTopicPartition NONE = new NetworkTopicPartition();
        static
        {
            NONE.id = -1;
        }

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
        private boolean compacted;
        BrokerMetadata[] brokers;
        private int nextBrokerIndex;
        private int[] nodeIdsByPartition;
        private List<Consumer<TopicMetadata>> consumers = new ArrayList<>();

        TopicMetadata(String topicName)
        {
            this.topicName = topicName;
        }

        public boolean hasMetadata()
        {
            return brokers != null;
        }

        public boolean isCompacted()
        {
            return compacted;
        }

        public void setCompacted(boolean compacted)
        {
            this.compacted = compacted;
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
