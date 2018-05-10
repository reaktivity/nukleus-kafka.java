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
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.INVALID_TOPIC_EXCEPTION;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.LEADER_NOT_AVAILABLE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.NONE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.OFFSET_OUT_OF_RANGE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.UNKNOWN_TOPIC_OR_PARTITION;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.reaktivity.nukleus.kafka.internal.function.IntBooleanConsumer;
import org.reaktivity.nukleus.kafka.internal.function.IntLongConsumer;
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
import org.reaktivity.nukleus.kafka.internal.types.codec.offset.IsolationLevel;
import org.reaktivity.nukleus.kafka.internal.types.codec.offset.ListOffsetsPartitionRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offset.ListOffsetsPartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offset.ListOffsetsRequestFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offset.ListOffsetsResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.offset.ListOffsetsTopicFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;

public final class NetworkConnectionPool
{
    private static final short FETCH_API_VERSION = 5;
    private static final short FETCH_API_KEY = 1;
    private static final short LIST_OFFSETS_API_KEY = 2;
    private static final short LIST_OFFSETS_API_VERSION = 2;
    private static final short METADATA_API_VERSION = 5;
    private static final short METADATA_API_KEY = 3;
    private static final short DESCRIBE_CONFIGS_API_VERSION = 0;
    private static final short DESCRIBE_CONFIGS_API_KEY = 32;

    private static final byte RESOURCE_TYPE_TOPIC = 2;
    private static final String CLEANUP_POLICY = "cleanup.policy";
    private static final String COMPACT = "compact";

    private static final int LOCAL_SLOT = -2;

    private static final byte[] ANY_IP_ADDR = new byte[4];

    private static final int MAX_PADDING = 5 * 1024;
    public static final MessageDispatcher NOOP_DISPATCHER = new MessageDispatcher()
    {

        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageOffset,
            DirectBuffer key,
            Function<DirectBuffer, DirectBuffer> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            return 1;
        }

        @Override
        public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
        {
        }

    };

    enum MetadataRequestType
    {
        METADATA, DESCRIBE_CONFIGS
    };

    final RequestHeaderFW.Builder requestRW = new RequestHeaderFW.Builder();
    final FetchRequestFW.Builder fetchRequestRW = new FetchRequestFW.Builder();
    final TopicRequestFW.Builder topicRequestRW = new TopicRequestFW.Builder();
    final PartitionRequestFW.Builder partitionRequestRW = new PartitionRequestFW.Builder();

    final MetadataRequestFW.Builder metadataRequestRW = new MetadataRequestFW.Builder();
    final DescribeConfigsRequestFW.Builder describeConfigsRequestRW = new DescribeConfigsRequestFW.Builder();
    final ListOffsetsRequestFW.Builder listOffsetsRequestRW = new ListOffsetsRequestFW.Builder();
    final ListOffsetsPartitionRequestFW.Builder listOffsetsPartitionRequestRW = new ListOffsetsPartitionRequestFW.Builder();
    final ListOffsetsTopicFW.Builder listOffsetsTopicRW = new ListOffsetsTopicFW.Builder();
    final ResourceRequestFW.Builder resourceRequestRW = new ResourceRequestFW.Builder();
    final String16FW.Builder configNameRW = new String16FW.Builder(ByteOrder.BIG_ENDIAN);

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
    final DescribeConfigsResponseFW describeConfigsResponseRO = new DescribeConfigsResponseFW();
    final ResourceResponseFW resourceResponseRO = new ResourceResponseFW();
    final ConfigResponseFW configResponseRO = new ConfigResponseFW();
    final ListOffsetsResponseFW listOffsetsResponseRO = new ListOffsetsResponseFW();
    final ListOffsetsTopicFW listOffsetsTopicRO = new ListOffsetsTopicFW();
    final ListOffsetsPartitionResponseFW listOffsetsPartitionResponseRO = new ListOffsetsPartitionResponseFW();

    final MutableDirectBuffer encodeBuffer;

    private final ClientStreamFactory clientStreamFactory;
    private final String networkName;
    private final long networkRef;
    private final int fetchMaxBytes;
    private final int fetchPartitionMaxBytes;
    private final BufferPool bufferPool;
    private AbstractFetchConnection[] connections = new LiveFetchConnection[0];
    private HistoricalFetchConnection[] historicalConnections = new HistoricalFetchConnection[0];
    private MetadataConnection metadataConnection;

    private final Map<String, TopicMetadata> topicMetadataByName;
    private final Map<String, NetworkTopic> topicsByName;
    private final Int2ObjectHashMap<Consumer<Long2LongHashMap>> detachersById;
    private final int maximumMessageSize;

    private int nextAttachId;

    NetworkConnectionPool(
        ClientStreamFactory clientStreamFactory,
        String networkName,
        long networkRef,
        int fetchMaxBytes,
        int fetchPartitionMaxBytes,
        BufferPool bufferPool)
    {
        this.clientStreamFactory = clientStreamFactory;
        this.networkName = networkName;
        this.networkRef = networkRef;
        this.fetchMaxBytes = fetchMaxBytes;
        this.fetchPartitionMaxBytes = fetchPartitionMaxBytes;
        this.bufferPool = bufferPool;
        this.encodeBuffer = new UnsafeBuffer(new byte[clientStreamFactory.bufferPool.slotCapacity()]);
        this.topicsByName = new LinkedHashMap<>();
        this.topicMetadataByName = new HashMap<>();
        this.detachersById = new Int2ObjectHashMap<>();

        // TODO: remove this and use multiple data frames to deliver large messages
        this.maximumMessageSize = bufferPool.slotCapacity() > MAX_PADDING ?
                bufferPool.slotCapacity() - MAX_PADDING : bufferPool.slotCapacity();
    }

    void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        int partitionHash,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        IntSupplier supplyWindow,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        Consumer<Consumer<IntBooleanConsumer>> finishAttachConsumer,
        IntConsumer onMetadataError)
    {
        final TopicMetadata metadata = topicMetadataByName.computeIfAbsent(topicName, TopicMetadata::new);
        metadata.doAttach(m -> doAttach(topicName, fetchOffsets, partitionHash, fetchKey,  headers, dispatcher, supplyWindow,
                    progressHandlerConsumer, finishAttachConsumer, onMetadataError, m));

        if (metadataConnection == null)
        {
            metadataConnection = new MetadataConnection();
        }

        metadataConnection.doRequestIfNeeded();
    }

    private void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        int partitionHash,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        IntSupplier supplyWindow,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        Consumer<Consumer<IntBooleanConsumer>> finishAttachConsumer,
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
                long offset = fetchOffsets.computeIfAbsent(0L, v -> 0L);
                long lowestOffset = topicMetadata.firstAvailableOffset(partitionId);
                offset = Math.max(offset,  lowestOffset);
                if (partitionId != 0)
                {
                    fetchOffsets.remove(0L);
                }
                fetchOffsets.put(partitionId, offset);
            }
            else
            {
                final int partitionCount = topicMetadata.partitionCount();
                for (int partition=0; partition < partitionCount; partition++)
                {
                    long offset = fetchOffsets.computeIfAbsent(partition, v -> 0L);
                    long lowestOffset = topicMetadata.firstAvailableOffset(partition);
                    offset = Math.max(offset,  lowestOffset);
                    fetchOffsets.put(partition, offset);
                }
            }
            final NetworkTopic topic = topicsByName.computeIfAbsent(topicName,
                    name -> new NetworkTopic(name, topicMetadata.partitionCount(), topicMetadata.compacted, false));
            finishAttachConsumer.accept(attachDetailsConsumer ->
            {
                progressHandlerConsumer.accept(topic.doAttach(fetchOffsets, fetchKey, headers, dispatcher, supplyWindow));
                final int newAttachId = nextAttachId++;
                detachersById.put(newAttachId, f -> topic.doDetach(f, fetchKey, headers, dispatcher, supplyWindow));
                doConnections(topicMetadata);
                attachDetailsConsumer.accept(newAttachId, topicMetadata.compacted);
            });

            break;
        default:
            throw new RuntimeException(format("Unexpected errorCode %d from metadata query", errorCode));
        }
    }

    public void doBootstrap(
        String topicName,
        IntConsumer onMetadataError)
    {
        final TopicMetadata metadata = topicMetadataByName.computeIfAbsent(topicName, TopicMetadata::new);
        metadata.doAttach(m -> doBootstrap(topicName, onMetadataError, m));

        if (metadataConnection == null)
        {
            metadataConnection = new MetadataConnection();
        }
        metadataConnection.doRequestIfNeeded();
    }

    private void doBootstrap(
        String topicName,
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
            if (topicMetadata.compacted)
            {
                topicsByName.computeIfAbsent(topicName,
                        name -> new NetworkTopic(name, topicMetadata.partitionCount(), topicMetadata.compacted, true));
                doConnections(topicMetadata);
                doFlush();
            }
            break;
        default:
            throw new RuntimeException(format("Unexpected errorCode %d from metadata query", errorCode));
        }
    }

    private void doConnections(TopicMetadata topicMetadata)
    {
        topicMetadata.visitBrokers(broker ->
        {
            connections = applyBrokerMetadata(connections, broker, LiveFetchConnection::new);
        });
        topicMetadata.visitBrokers(broker ->
        {
            historicalConnections = applyBrokerMetadata(historicalConnections, broker,  HistoricalFetchConnection::new);
        });
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
        long networkCorrelationId;

        int networkRequestBudget;
        int networkRequestPadding;

        int networkResponseBudget;

        MessageConsumer networkReplyThrottle;
        long networkReplyId;

        private MessageConsumer streamState;

        int networkSlot = NO_SLOT;
        int networkSlotOffset;

        int nextRequestId;
        int nextResponseId;

        final MutableDirectBuffer localDecodeBuffer;

        private AbstractNetworkConnection()
        {
            this.networkTarget = NetworkConnectionPool.this.clientStreamFactory.router.supplyTarget(networkName);
            localDecodeBuffer = new UnsafeBuffer(allocateDirect(fetchPartitionMaxBytes));

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
            doBeginIfNotConnected((b, o, m) -> 0);
        }

        final void doBeginIfNotConnected(
            Flyweight.Builder.Visitor extensionVisitor)
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
                this.networkCorrelationId = newCorrelationId;
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
            final int networkCredit = window.credit();
            final int networkPadding = window.padding();

            this.networkRequestBudget += networkCredit;
            this.networkRequestPadding = networkPadding;

            doRequestIfNeeded();
        }

        private void handleReset(
            ResetFW reset)
        {
            NetworkConnectionPool.this.clientStreamFactory.correlations.remove(
                    networkCorrelationId, AbstractNetworkConnection.this);
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


        void handleData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            final long networkTraceId = data.trace();

            networkResponseBudget -= payload.sizeof() + data.padding();

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
                                networkSlot == LOCAL_SLOT ? localDecodeBuffer : bufferPool.buffer(networkSlot);
                        final int networkRemaining = networkLimit - networkOffset;

                        bufferSlot.putBytes(networkSlotOffset, networkBuffer, networkOffset, networkRemaining);
                        networkSlotOffset += networkRemaining;

                        networkBuffer = bufferSlot;
                        networkOffset = 0;
                        networkLimit = networkSlotOffset;
                    }

                    ResponseHeaderFW response = null;
                    int responseSize = 0;
                    if (networkOffset + BitUtil.SIZE_OF_INT <= networkLimit)
                    {
                        response = NetworkConnectionPool.this.responseRO.wrap(networkBuffer, networkOffset, networkLimit);
                        networkOffset = response.limit();
                        responseSize = response.size();
                    }

                    if (response == null || networkOffset + responseSize > networkLimit)
                    {
                        if (networkSlot == NO_SLOT)
                        {
                            final MutableDirectBuffer bufferSlot = localDecodeBuffer;
                            networkSlotOffset = 0;
                            networkSlot = LOCAL_SLOT;
                            bufferSlot.putBytes(networkSlotOffset, payload.buffer(), payload.offset(), payload.sizeof());
                            networkSlotOffset += payload.sizeof();

                            doOfferResponseBudget();
                        }
                    }
                    else
                    {
                        handleResponse(networkTraceId, networkBuffer, networkOffset, networkLimit);
                        networkOffset += response.size();
                        nextResponseId++;

                        if (networkOffset < networkLimit)
                        {
                            if (networkSlot == NO_SLOT)
                            {
                                networkSlot = bufferPool.acquire(networkReplyId);
                            }

                            final MutableDirectBuffer bufferSlot =
                                    bufferPool.buffer(networkSlot);
                            bufferSlot.putBytes(0, networkBuffer, networkOffset, networkLimit - networkOffset);
                            networkSlotOffset = networkLimit - networkOffset;
                        }
                        else
                        {
                            assert networkOffset == networkLimit;
                            networkSlotOffset = 0;
                            if (networkSlot == LOCAL_SLOT)
                            {
                                // so that response budget credit is computed w.r.t bufferPool slot
                                networkSlot = NO_SLOT;
                            }
                        }

                        doOfferResponseBudget();
                        doRequestIfNeeded();
                    }
                }
                finally
                {
                    if (networkSlotOffset == 0 && networkSlot != NO_SLOT)
                    {
                        networkSlot = NO_SLOT;
                    }
                }
            }
        }

        abstract void handleResponse(
            long networkTraceId,
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
            final int slotCapacity = localDecodeBuffer.capacity();
            final int networkResponseCredit =
                    Math.max(slotCapacity - networkResponseBudget, 0);

            if (networkResponseCredit > 0)
            {
                NetworkConnectionPool.this.clientStreamFactory.doWindow(
                        networkReplyThrottle, networkReplyId, networkResponseCredit, 0, 0);

                this.networkResponseBudget += networkResponseCredit;
            }
        }

        void doReinitialize()
        {
            networkSlotOffset = 0;
            networkRequestBudget = 0;
            networkRequestPadding = 0;
            networkResponseBudget = 0;
            networkId = 0L;
            nextRequestId = 0;
            nextResponseId = 0;
        }
    }

    abstract class AbstractFetchConnection extends AbstractNetworkConnection
    {
        private static final long EARLIEST_AVAILABLE_OFFSET = -2L;

        final String host;
        final int port;
        final int brokerId;

        int encodeLimit;
        boolean offsetsNeeded;
        boolean offsetsRequested;
        Map<String, long[]> requestedFetchOffsetsByTopic = new HashMap<>();
        final ResponseDecoder fetchResponseDecoder;

        private AbstractFetchConnection(BrokerMetadata broker)
        {
            super();
            this.brokerId = broker.nodeId;
            this.host = broker.host;
            this.port = broker.port;
            fetchResponseDecoder = new FetchResponseDecoder(
                    this::getTopicDispatcher,
                    this::getRequestedOffset,
                    this::handlePartitionResponseError,
                    localDecodeBuffer,
                    maximumMessageSize);
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
                    if (offsetsNeeded)
                    {
                        doListOffsetsRequest();
                        offsetsRequested = true;
                    }
                    else
                    {
                        doFetchRequest();
                    }
                }
            }
        }

        private void doFetchRequest()
        {
            final int encodeOffset = 0;
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
                    .maxBytes(fetchMaxBytes)
                    .isolationLevel((byte) 0)
                    .topicCount(0)
                    .build();

            encodeLimit = fetchRequest.limit();
            int topicCount = 0;
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

                long[] requestedOffsets = requestedFetchOffsetsByTopic.computeIfAbsent(
                        topicName,
                        k  ->  new long[topicMetadataByName.get(topicName).partitionCount()]);
                int partitionCount = addTopicToRequest(topicName, (p, o) ->
                {
                    requestedOffsets[p] = o;
                });

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

            // TODO: stream large requests in multiple DATA frames as needed
            if (topicCount > 0 && encodeLimit - encodeOffset + networkRequestPadding <= networkRequestBudget)
            {
                NetworkConnectionPool.this.fetchRequestRW
                    .wrap(NetworkConnectionPool.this.encodeBuffer, fetchRequest.offset(), fetchRequest.limit())
                    .maxWaitTimeMillis(fetchRequest.maxWaitTimeMillis())
                    .minBytes(fetchRequest.minBytes())
                    .maxBytes(fetchRequest.maxBytes())
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

                OctetsFW payload = NetworkConnectionPool.this.payloadRW
                    .wrap(NetworkConnectionPool.this.encodeBuffer, encodeOffset, encodeLimit)
                    .set((b, o, m) -> m - o)
                    .build();

                NetworkConnectionPool.this.clientStreamFactory.doData(networkTarget, networkId,
                        networkRequestPadding, payload);
                networkRequestBudget -= payload.sizeof() + networkRequestPadding;
            }
        }

        private void doListOffsetsRequest()
        {

            final int encodeOffset = 0;
            int encodeLimit = encodeOffset;

            RequestHeaderFW request = requestRW.wrap(
                    NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                    NetworkConnectionPool.this.encodeBuffer.capacity())
                    .size(0)
                    .apiKey(LIST_OFFSETS_API_KEY)
                    .apiVersion(LIST_OFFSETS_API_VERSION)
                    .correlationId(0)
                    .clientId((String) null)
                    .build();

            encodeLimit = request.limit();

            ListOffsetsRequestFW listOffsetsRequest = listOffsetsRequestRW.wrap(
                    NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                    NetworkConnectionPool.this.encodeBuffer.capacity())
                    .isolationLevel(b -> b.set(IsolationLevel.READ_UNCOMMITTED))
                    .topicCount(0)
                    .build();

            encodeLimit = listOffsetsRequest.limit();

            int topicCount = 0;

            for (TopicMetadata topicMetadata : topicMetadataByName.values())
            {
                if (topicMetadata.offsetsRequired(brokerId))
                {
                    int partitionCount = topicMetadata.partitionCount(brokerId);
                    if (partitionCount > 0)
                    {
                        topicCount++;
                        ListOffsetsTopicFW listOffsetsTopic = listOffsetsTopicRW.wrap(
                                NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                NetworkConnectionPool.this.encodeBuffer.capacity())
                                .name(topicMetadata.topicName)
                                .partitionCount(partitionCount)
                                .build();

                        encodeLimit = listOffsetsTopic.limit();

                        for (int partitionId=0; partitionId <  topicMetadata.nodeIdsByPartition.length; partitionId++)
                        {
                            if (topicMetadata.nodeIdsByPartition[partitionId] == brokerId)
                            {
                                ListOffsetsPartitionRequestFW listOffsetsPartitionRequest = listOffsetsPartitionRequestRW.wrap(
                                        NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                        NetworkConnectionPool.this.encodeBuffer.capacity())
                                        .partitionId(partitionId)
                                        .timestamp(EARLIEST_AVAILABLE_OFFSET)
                                        .build();

                                encodeLimit = listOffsetsPartitionRequest.limit();
                            }
                        }
                    }
                }
            }

            // TODO: stream large requests in multiple DATA frames as needed
            if (encodeLimit - encodeOffset + networkRequestPadding <= networkRequestBudget)
            {
                int newCorrelationId = nextRequestId++;

                NetworkConnectionPool.this.requestRW
                         .wrap(NetworkConnectionPool.this.encodeBuffer, request.offset(), request.limit())
                         .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                         .apiKey(LIST_OFFSETS_API_KEY)
                         .apiVersion(LIST_OFFSETS_API_VERSION)
                         .correlationId(newCorrelationId)
                         .clientId((String) null)
                         .build();

                listOffsetsRequestRW.wrap(encodeBuffer, listOffsetsRequest.offset(), listOffsetsRequest.limit())
                        .isolationLevel(b -> b.set(IsolationLevel.READ_UNCOMMITTED))
                        .topicCount(topicCount)
                        .build();

                OctetsFW payload = NetworkConnectionPool.this.payloadRW
                        .wrap(NetworkConnectionPool.this.encodeBuffer, encodeOffset, encodeLimit)
                        .set((b, o, m) -> m - o)
                        .build();

                NetworkConnectionPool.this.clientStreamFactory.doData(networkTarget, networkId,
                        networkRequestPadding, payload);
                networkRequestBudget -= payload.sizeof() + networkRequestPadding;
            }
        }

        abstract int addTopicToRequest(
            String topicName,
            IntLongConsumer partitionsOffsets);

        @Override
        void handleData(
            DataFW data)
        {
            if (offsetsRequested)
            {
                super.handleData(data);
            }
            else
            {
                final OctetsFW payload = data.payload();
                networkResponseBudget -= payload.sizeof() + data.padding();
                if (networkResponseBudget < 0)
                {
                    NetworkConnectionPool.this.clientStreamFactory.doReset(networkReplyThrottle, networkReplyId);
                }
                int excessBytes = fetchResponseDecoder.decode(payload, data.trace());
                doOfferResponseBudget();
                if (excessBytes >= 0) // response complete
                {
                    assert excessBytes == 0 : "bytes remaining after fetch response, pipelined requests are not being used";
                    nextResponseId++;
                    doRequestIfNeeded();
                }
            }
        }

        @Override
        final void handleResponse(
            long networkTraceId,
            DirectBuffer networkBuffer,
            int networkOffset,
            int networkLimit)
        {
            assert offsetsNeeded;
            handleListOffsetsResponse(networkTraceId, networkBuffer, networkOffset, networkLimit);
            offsetsNeeded = false;
            offsetsRequested = false;
        }

        final void handleListOffsetsResponse(
            long networkTraceId,
            DirectBuffer networkBuffer,
            int networkOffset,
            int networkLimit)
        {
            final ListOffsetsResponseFW response =
                    NetworkConnectionPool.this.listOffsetsResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
            int topicCount = response.topicCount();
            networkOffset = response.limit();

            short errorCode = NONE;
            for (int topicIndex = 0; topicIndex < topicCount && errorCode == NONE; topicIndex++)
            {
                final ListOffsetsTopicFW topic =
                        listOffsetsTopicRO.wrap(networkBuffer, networkOffset, networkLimit);
                final String16FW name = topic.name();
                TopicMetadata topicMetadata = topicMetadataByName.get(name.asString());
                networkOffset = topic.limit();

                final int partitionCount = topic.partitionCount();

                for (int i = 0; i < partitionCount && errorCode == NONE; i++)
                {
                    final ListOffsetsPartitionResponseFW partition =
                            listOffsetsPartitionResponseRO.wrap(networkBuffer, networkOffset, networkLimit);
                    errorCode = partition.errorCode();
                    if (errorCode != NONE)
                    {
                        break;
                    }
                    final long offset = partition.firstOffset();
                    final int partitionId = partition.partitionId();
                    topicMetadata.setFirstOffset(partitionId, offset);
                    networkOffset = partition.limit();
                }
            }
        }

        final long getRequestedOffset(String topicName, int partitionId)
        {
            return requestedFetchOffsetsByTopic.get(topicName)[partitionId];
        }

        @Override
        void doReinitialize()
        {
            fetchResponseDecoder.reinitialize();
            super.doReinitialize();
        }

        private MessageDispatcher getTopicDispatcher(String topicName)
        {
            NetworkTopic topic = topicsByName.get(topicName);

            // topic may be null if everyone has unsubscribed
            return topic == null ? NOOP_DISPATCHER : topic.dispatcher;
        }

        private void handlePartitionResponseError(
            String topicName,
            int partition,
            short errorCode)
        {
            switch(errorCode)
            {
            case OFFSET_OUT_OF_RANGE:
                offsetsNeeded = true;
                TopicMetadata topicMetadata = topicMetadataByName.get(topicName);

                // logStartOffset is always -1 in this case so we can't use it
                topicMetadata.setFirstOffset(partition, TopicMetadata.UNKNOWN_OFFSET);

                break;
            default:
                throw new IllegalStateException(format(
                    "%s: unexpected error code %d from fetch, topic %s, partition %d, requested offset %d",
                    this, errorCode, topicName, partition, getRequestedOffset(topicName, partition)));
            }
        }

        @Override
        public String toString()
        {
            return String.format("%s [brokerId=%d, host=%s, port=%d, budget=%d, padding=%d]",
                    getClass().getName(), brokerId, host, port, networkRequestBudget, networkRequestPadding);
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
            String topicName,
            IntLongConsumer partitionOffsets)
        {
            final NetworkTopic topic = topicsByName.get(topicName);
            final int maxPartitionBytes = topic.maximumWritableBytes(true);

            int partitionCount = 0;

            if (maxPartitionBytes > 0)
            {
                final TopicMetadata metadata = topicMetadataByName.get(topicName);
                final int[] nodeIdsByPartition = metadata.nodeIdsByPartition;

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
                        long offset = Math.max(candidate.offset,  metadata.firstAvailableOffset(candidate.id));
                        PartitionRequestFW partitionRequest = NetworkConnectionPool.this.partitionRequestRW
                            .wrap(NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                    NetworkConnectionPool.this.encodeBuffer.capacity())
                            .partitionId(candidate.id)
                            .fetchOffset(offset)
                            .maxBytes(maxPartitionBytes)
                            .build();

                        partitionOffsets.accept(candidate.id,  candidate.offset);
                        encodeLimit = partitionRequest.limit();
                        partitionCount++;
                    }
                    candidate = next;
                }
            }

            return partitionCount;
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
            String topicName,
            IntLongConsumer partitionOffsets)
        {
            int partitionCount = 0;

            NetworkTopic topic = topicsByName.get(topicName);
            if (topic.needsHistorical())
            {
                final int maxPartitionBytes = topic.maximumWritableBytes(false);
                if (maxPartitionBytes > 0)
                {
                    final TopicMetadata metadata = topicMetadataByName.get(topicName);
                    final int[] nodeIdsByPartition = metadata.nodeIdsByPartition;

                    int partitionId = -1;
                    // TODO: eliminate iterator allocation
                    for (NetworkTopicPartition partition : topic.partitions)
                    {
                        if (topic.needsHistorical(partition.id) &&
                                partitionId < partition.id && nodeIdsByPartition[partition.id] == brokerId)
                        {
                            long offset = Math.max(partition.offset,  metadata.firstAvailableOffset(partition.id));
                            PartitionRequestFW partitionRequest = NetworkConnectionPool.this.partitionRequestRW
                                .wrap(NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                        NetworkConnectionPool.this.encodeBuffer.capacity())
                                .partitionId(partition.id)
                                .fetchOffset(offset)
                                .maxBytes(maxPartitionBytes)
                                .build();

                            partitionOffsets.accept(partition.id,  partition.offset);
                            encodeLimit = partitionRequest.limit();
                            partitionId = partition.id;
                            partitionCount++;
                        }
                    }
                }
            }

            return partitionCount;
        }
    }

    private final class MetadataConnection extends AbstractNetworkConnection
    {
        TopicMetadata pendingTopicMetadata;
        MetadataRequestType pendingRequest = MetadataRequestType.METADATA;

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
                    switch(pendingTopicMetadata.nextRequiredRequestType())
                    {
                    case DESCRIBE_CONFIGS:
                        doDescribeConfigsRequest();
                        break;
                    case METADATA:
                        doMetadataRequest();
                        break;
                    }
                }
            }
        }

        private void doDescribeConfigsRequest()
        {
            doBeginIfNotConnected();

            if (networkRequestBudget > networkRequestPadding)
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
                if (encodeLimit - encodeOffset + networkRequestPadding <= networkRequestBudget)
                {
                    int newCorrelationId = nextRequestId++;

                    NetworkConnectionPool.this.requestRW
                             .wrap(buffer, request.offset(), request.limit())
                             .size(encodeLimit - encodeOffset - RequestHeaderFW.FIELD_OFFSET_API_KEY)
                             .apiKey(DESCRIBE_CONFIGS_API_KEY)
                             .apiVersion(DESCRIBE_CONFIGS_API_VERSION)
                             .correlationId(newCorrelationId)
                             .clientId((String) null)
                             .build();

                    OctetsFW payload = NetworkConnectionPool.this.payloadRW
                            .wrap(buffer, encodeOffset, encodeLimit)
                            .set((b, o, m) -> m - o)
                            .build();

                    NetworkConnectionPool.this.clientStreamFactory.doData(networkTarget, networkId,
                            networkRequestPadding, payload);
                    networkRequestBudget -= payload.sizeof() + networkRequestPadding;
                    pendingRequest = MetadataRequestType.DESCRIBE_CONFIGS;
                }
            }
        }

        private void doMetadataRequest()
        {
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
                        .topicName(pendingTopicMetadata.topicName)
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

                    NetworkConnectionPool.this.clientStreamFactory.doData(networkTarget, networkId,
                            networkRequestPadding, payload);
                    networkRequestBudget -= payload.sizeof() + networkRequestPadding;
                    pendingRequest = MetadataRequestType.METADATA;
                }
            }
        }

        @Override
        void handleResponse(
            long networkTraceId,
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
            switch(pendingRequest)
            {
            case DESCRIBE_CONFIGS:
                handleDescribeConfigsResponse(networkTraceId, networkBuffer, networkOffset, networkLimit);
                break;
            case METADATA:
                handleMetadataResponse(networkTraceId, networkBuffer, networkOffset, networkLimit);
                break;
            }
        }

        void handleDescribeConfigsResponse(
            long networkTraceId,
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
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
            if (errorCode != NONE && KafkaErrors.isRecoverable(errorCode))
            {
                pendingTopicMetadata.reset();
                doRequestIfNeeded();
            }
            else
            {
                TopicMetadata metadata = pendingTopicMetadata;
                pendingTopicMetadata = null;
                metadata.setErrorCode(errorCode);
                metadata.setCompacted(compacted);
                metadata.setCompleted();
                metadata.flush();
            }
        }

        void handleMetadataResponse(
            long networkTraceId,
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
            if (errorCode == NONE)
            {
                pendingTopicMetadata.nextRequiredRequestType = MetadataRequestType.DESCRIBE_CONFIGS;
            }
            else
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
        private final boolean compacted;
        private final Set<IntSupplier> windowSuppliers;
        final NavigableSet<NetworkTopicPartition> partitions;
        private final NetworkTopicPartition candidate;
        private final TopicMessageDispatcher dispatcher;
        private final PartitionProgressHandler progressHandler;

        private BitSet needsHistoricalByPartition = new BitSet();
        private final boolean proactive;

        private final class BootstrapMessageDispatcher  implements MessageDispatcher
        {
            private final long[] offsets;

            BootstrapMessageDispatcher(int partitions)
            {
                offsets = new long[partitions];
            }

            @Override
            public int dispatch(
                int partition,
                long requestOffset,
                long messageOffset,
                DirectBuffer key,
                java.util.function.Function<DirectBuffer, DirectBuffer> supplyHeader,
                long timestamp,
                long traceId,
                DirectBuffer value)
            {
                return 1;
            };

            @Override
            public void flush(
                int partition,
                long requestOffset,
                long lastOffset)
            {
                progressHandler.handle(partition, offsets[partition], lastOffset);
                offsets[partition] = lastOffset;
            };
        };

        @Override
        public String toString()
        {
            return String.format("topicName=%s, partitions=%s", topicName, partitions);
        }

        NetworkTopic(
            String topicName,
            int partitionCount,
            boolean compacted,
            boolean proactive)
        {
            this.topicName = topicName;
            this.compacted = compacted;
            this.windowSuppliers = new HashSet<>();
            this.partitions = new TreeSet<>();
            this.candidate = new NetworkTopicPartition();
            this.progressHandler = this::handleProgress;
            this.dispatcher = new TopicMessageDispatcher(partitionCount,
                    compacted ? CachingKeyMessageDispatcher::new : KeyMessageDispatcher::new,
                    compacted ? CompactedHeaderValueMessageDispatcher::new : HeaderValueMessageDispatcher::new);
            this.proactive = proactive;
            if (proactive)
            {
                 for (int i=0; i < partitionCount; i++)
                 {
                     attachToPartition(i, 0L, 1);
                 }
                 MessageDispatcher bootstrapDispatcher = new BootstrapMessageDispatcher(partitionCount);
                 this.dispatcher.add(null, -1, null, bootstrapDispatcher);
            }
        }

        PartitionProgressHandler doAttach(
            Long2LongHashMap fetchOffsets,
            OctetsFW fetchKey,
            ListFW<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher,
            IntSupplier supplyWindow)
        {
            windowSuppliers.add(supplyWindow);
            int fetchKeyPartition;
            if (fetchKey == null)
            {
                fetchKeyPartition = -1;
                final LongIterator keys = fetchOffsets.keySet().iterator();
                while (keys.hasNext())
                {
                    final int partitionId = (int) keys.nextValue();
                    long fetchOffset = fetchOffsets.get(partitionId);
                    long cachedOffset = this.dispatcher.lowestOffset(partitionId);
                    if (cachedOffset > fetchOffset)
                    {
                        fetchOffsets.put(partitionId, cachedOffset);
                        fetchOffset = cachedOffset;
                    }
                    attachToPartition(partitionId, fetchOffset, 1);
                }
            }
            else
            {
                fetchKeyPartition = fetchOffsets.keySet().iterator().next().intValue();
                long fetchOffset = fetchOffsets.get(fetchKeyPartition);
                if (compacted)
                {
                    long cachedOffset = this.dispatcher.lastOffset(fetchKeyPartition, fetchKey);
                    if (cachedOffset > fetchOffset)
                    {
                        fetchOffsets.put(fetchKeyPartition, cachedOffset);
                        fetchOffset = cachedOffset;
                    }
                }
                attachToPartition(fetchKeyPartition, fetchOffset, 1);
            }

            this.dispatcher.add(fetchKey, fetchKeyPartition, headers, dispatcher);
            return progressHandler;
        }

        private void attachToPartition(
            int partitionId,
            long fetchOffset,
            final int refs)
        {
            candidate.id = partitionId;
            candidate.offset = fetchOffset;
            NetworkTopicPartition partition = partitions.floor(candidate);
            if (!candidate.equals(partition))
            {
                boolean needsHistorical;
                if (partition != null && partition.id == candidate.id)
                {
                    needsHistorical = true;
                }
                else
                {
                    NetworkTopicPartition ceiling = partitions.ceiling(candidate);
                    needsHistorical = ceiling != null && ceiling.id == candidate.id;
                }
                needsHistoricalByPartition.set(candidate.id, needsHistorical);
                partition = new NetworkTopicPartition();
                partition.id = candidate.id;
                partition.offset = candidate.offset;

                partitions.add(partition);
            }
            partition.refs += refs;
        }

        void doDetach(
            Long2LongHashMap fetchOffsets,
            OctetsFW fetchKey,
            ListFW<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher,
            IntSupplier supplyWindow)
        {
            windowSuppliers.remove(supplyWindow);
            int fetchKeyPartition = (int) (fetchKey == null ? -1 : fetchOffsets.keySet().iterator().next());
            this.dispatcher.remove(fetchKey, fetchKeyPartition, headers, dispatcher);
            final LongIterator partitionIds = fetchOffsets.keySet().iterator();
            while (partitionIds.hasNext())
            {
                long partitionId = partitionIds.nextValue();
                doDetach((int) partitionId, fetchOffsets.get(partitionId), supplyWindow);
            }
            if (partitions.isEmpty()  && !compacted)
            {
                topicsByName.remove(topicName);
                topicMetadataByName.remove(topicName);
            }
        }

        void doDetach(
            int partitionId,
            long fetchOffset,
            IntSupplier supplyWindow)
        {
            windowSuppliers.remove(supplyWindow);
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

        int maximumWritableBytes(boolean live)
        {
            int writableBytes = 0;
            if (live && proactive)
            {
                writableBytes = fetchPartitionMaxBytes;
            }
            else
            {
                // TODO: eliminate iterator allocation
                for (IntSupplier supplyWindow : windowSuppliers)
                {
                    writableBytes = Math.max(writableBytes, supplyWindow.getAsInt());
                }
                writableBytes = Math.min(writableBytes, maximumMessageSize);
            }
            return writableBytes;
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
        public boolean equals(
            Object arg0)
        {
            boolean result = false;
            if (this == arg0)
            {
                result = true;
            }
            else if (arg0 != null && arg0.getClass() == NetworkTopicPartition.class)
            {
                NetworkTopicPartition other = (NetworkTopicPartition) arg0;
                result = this.id == other.id && this.offset == other.offset;
            }
            return result;
        }

        @Override
        public int hashCode()
        {
            int offsetHash = id ^ (id >>> 32);
            return 31 * id + offsetHash;
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
        private static final long UNKNOWN_OFFSET = -1L;

        private final String topicName;
        private short errorCode;
        private boolean compacted;
        private boolean completed;
        BrokerMetadata[] brokers;
        private int nextBrokerIndex;
        private int[] nodeIdsByPartition;
        private long[] firstOffsetsByPartition;
        private List<Consumer<TopicMetadata>> consumers = new ArrayList<>();
        private MetadataRequestType nextRequiredRequestType = MetadataRequestType.METADATA;

        TopicMetadata(String topicName)
        {
            this.topicName = topicName;
        }

        MetadataRequestType nextRequiredRequestType()
        {
            return nextRequiredRequestType;
        }

        void setCompacted(boolean compacted)
        {
            this.compacted = compacted;
        }

        void setCompleted()
        {
            this.completed = true;
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
            firstOffsetsByPartition = new long[partitionCount];
        }

        void addPartition(int partitionId, int nodeId)
        {
            nodeIdsByPartition[partitionId] = nodeId;
        }

        long firstAvailableOffset(
            int partitionId)
        {
            return firstOffsetsByPartition[partitionId];
        }

        boolean offsetsRequired(
            int nodeId)
        {
            boolean result = false;
            if (nodeIdsByPartition != null)
            {
                for (int i=0; i < nodeIdsByPartition.length; i++)
                {
                    if (nodeIdsByPartition[i] == nodeId &&
                            firstOffsetsByPartition[i] == UNKNOWN_OFFSET)
                    {
                        result = true;
                        break;
                    }
                }
            }
            return result;
        }

        void setFirstOffset(int partitionId, long offset)
        {
            firstOffsetsByPartition[partitionId] = offset;
        }

        void doAttach(
            Consumer<TopicMetadata> consumer)
        {
            consumers.add(consumer);
            if (completed)
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

        int partitionCount(int nodeId)
        {
            int result = 0;
            for (int i=0; i < nodeIdsByPartition.length; i++)
            {
                if (nodeIdsByPartition[i] == nodeId)
                {
                    result++;
                }
            }
            return result;
        }

        void reset()
        {
            brokers = null;
            nodeIdsByPartition = null;
            nextBrokerIndex = 0;
        }
    }

}
