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

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyIterator;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaError.NONE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaError.UNEXPECTED_SERVER_ERROR;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaError.asKafkaError;
import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.IntToLongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.TimerWheel.Timer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.LongIterator;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaRefCounters;
import org.reaktivity.nukleus.kafka.internal.cache.CompactedPartitionIndex;
import org.reaktivity.nukleus.kafka.internal.cache.DefaultPartitionIndex;
import org.reaktivity.nukleus.kafka.internal.cache.MessageCache;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.function.IntLongConsumer;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;
import org.reaktivity.nukleus.kafka.internal.util.DelayedTaskScheduler;

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

    private static final long MAX_OFFSET = Long.MAX_VALUE;
    private static final long NO_OFFSET = -1L;

    private static final byte RESOURCE_TYPE_TOPIC = 2;
    private static final String CLEANUP_POLICY = "cleanup.policy";
    private static final String DELETE_RETENTION_MS = "delete.retention.ms";
    private static final String COMPACT = "compact";

    private static final int LOCAL_SLOT = -2;

    private static final byte[] ANY_IP_ADDR = new byte[4];

    private static final int KAFKA_SERVER_DEFAULT_DELETE_RETENTION_MS = 86400000;
    private static final PartitionIndex DEFAULT_PARTITION_INDEX = new DefaultPartitionIndex();

    private static final LongSupplier NO_COUNTER = Long.valueOf(0L)::longValue;

    private static final DecoderMessageDispatcher NOOP_DISPATCHER = new DecoderMessageDispatcher()
    {
        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageOffset,
            long highWatermark,
            DirectBuffer key,
            HeadersFW headers,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            return 0;
        }

        @Override
        public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
        {
        }

    };

    private static final MessageDispatcher MATCHING_MESSAGE_DISPATCHER = new MessageDispatcher()
    {
        @Override
        public void adjustOffset(int partition, long oldOffset, long newOffset)
        {

        }

        @Override
        public void detach(
            boolean reattach)
        {
        }

        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageOffset,
            DirectBuffer key,
            java.util.function.Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            return MessageDispatcher.FLAGS_MATCHED;
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
    final PartitionRequestFW partitionRequestRO = new PartitionRequestFW();

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
    private final int readIdleTimeout;
    private final BufferPool bufferPool;

    private final MessageCache messageCache;
    private final boolean forceProactiveMessageCache;
    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final UnsafeBuffer valueBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final MessageFW messageRO = new MessageFW();
    private final HeadersFW headersRO = new HeadersFW();
    private final KafkaHeadersIterator headersIterator = new KafkaHeadersIterator();

    private AbstractFetchConnection[] connections = new LiveFetchConnection[0];
    private HistoricalFetchConnection[] historicalConnections = new HistoricalFetchConnection[0];
    private MetadataConnection metadataConnection;
    private final Backoff metadataBackoffMillis;

    private final Map<String, TopicMetadata> topicMetadataByName;
    private final Map<String, NetworkTopic> topicsByName;
    private final Map<String, List<ListFW<KafkaHeaderFW>>> routeHeadersByTopic;
    private final Int2ObjectHashMap<Consumer<Long2LongHashMap>> detachersById;

    private final List<NetworkTopicPartition> partitionsWorkList = new ArrayList<NetworkTopicPartition>();
    private final LongArrayList offsetsWorkList = new LongArrayList();
    private final KafkaRefCounters routeCounters;

    private int nextAttachId;

    NetworkConnectionPool(
        ClientStreamFactory clientStreamFactory,
        String networkName,
        long networkRef,
        int fetchMaxBytes,
        int fetchPartitionMaxBytes,
        BufferPool bufferPool,
        MessageCache messageCache,
        Function<String, LongSupplier> supplyCounter,
        boolean forceProactiveMessageCache,
        int readIdleTimeout)
    {
        this.clientStreamFactory = clientStreamFactory;
        this.networkName = networkName;
        this.networkRef = networkRef;
        this.fetchMaxBytes = fetchMaxBytes;
        this.fetchPartitionMaxBytes = fetchPartitionMaxBytes;
        this.bufferPool = bufferPool;
        this.messageCache = messageCache;
        this.forceProactiveMessageCache = forceProactiveMessageCache;
        this.routeCounters = clientStreamFactory.counters.supplyRef(networkName, networkRef);
        this.encodeBuffer = new UnsafeBuffer(new byte[clientStreamFactory.bufferPool.slotCapacity()]);
        this.topicsByName = new LinkedHashMap<>();
        this.topicMetadataByName = new HashMap<>();
        this.routeHeadersByTopic = new HashMap<>();
        this.detachersById = new Int2ObjectHashMap<>();
        this.readIdleTimeout = readIdleTimeout;
        this.metadataBackoffMillis = new Backoff(10, 10_000);
    }

    int doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        int partitionHash,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        IntSupplier supplyWindow,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        Consumer<Consumer<Consumer<Boolean>>> finishAttachConsumer,
        Consumer<KafkaError> onMetadataError)
    {
        final TopicMetadata metadata = topicMetadataByName.computeIfAbsent(topicName, TopicMetadata::new);
        final int newAttachId = nextAttachId++;
        metadata.doAttach(
                newAttachId,
                m -> doAttach(topicName, fetchOffsets, partitionHash, fetchKey, headers, newAttachId,
                dispatcher, supplyWindow, progressHandlerConsumer, finishAttachConsumer, onMetadataError, m));

        if (metadataConnection == null)
        {
            metadataConnection = new MetadataConnection();
        }

        metadataConnection.doRequestIfNeeded();
        return newAttachId;
    }

    private void doAttach(
        String topicName,
        Long2LongHashMap fetchOffsets,
        int partitionHash,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        int attachId,
        MessageDispatcher dispatcher,
        IntSupplier supplyWindow,
        Consumer<PartitionProgressHandler> progressHandlerConsumer,
        Consumer<Consumer<Consumer<Boolean>>> finishAttachConsumer,
        Consumer<KafkaError> onMetadataError,
        TopicMetadata topicMetadata)
    {
        KafkaError errorCode = topicMetadata.errorCode();
        switch(errorCode)
        {
        case INVALID_TOPIC_EXCEPTION:
        case UNKNOWN_TOPIC_OR_PARTITION:
            onMetadataError.accept(errorCode);
            break;
        case NONE:
            // For streaming topics default to receiving only live (new) messages
            final long defaultOffset = fetchOffsets.isEmpty() && !topicMetadata.compacted ? MAX_OFFSET : 0L;

            if (fetchKey != null)
            {
                int partitionId = BufferUtil.partition(partitionHash, topicMetadata.partitionCount());
                long offset = fetchOffsets.computeIfAbsent(0L, v -> defaultOffset);
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
                    long offset = fetchOffsets.computeIfAbsent(partition, v -> defaultOffset);
                    long lowestOffset = topicMetadata.firstAvailableOffset(partition);
                    offset = Math.max(offset,  lowestOffset);
                    fetchOffsets.put(partition, offset);
                }
            }
            final NetworkTopic topic = topicsByName.computeIfAbsent(topicName,
                    name -> new NetworkTopic(name, topicMetadata.partitionCount(), topicMetadata.compacted, false,
                            topicMetadata.deleteRetentionMs, false));
            finishAttachConsumer.accept(attachDetailsConsumer ->
            {
                progressHandlerConsumer.accept(
                        topic.doAttach(fetchOffsets, fetchKey, headers, dispatcher, supplyWindow));
                detachersById.put(attachId, f -> topic.doDetach(f, fetchKey, headers, dispatcher, supplyWindow));
                doConnections(topicMetadata);
                attachDetailsConsumer.accept(topicMetadata.compacted);
            });

            break;
        default:
            throw new RuntimeException(format("Unexpected errorCode %s from metadata query for topic %s",
                    errorCode, topicName));
        }
    }

    public void addRoute(
        String topicName,
        ListFW<KafkaHeaderFW> routeHeaders,
        boolean proactive,
        BiConsumer<KafkaError, String> onMetadataError)
    {
        final TopicMetadata metadata = topicMetadataByName.computeIfAbsent(topicName, TopicMetadata::new);

        if (proactive)
        {
            int newAttachId = nextAttachId++;
            metadata.doAttach(
                    newAttachId,
                    m -> addRoute(topicName, routeHeaders, proactive, onMetadataError, m));
            if (metadataConnection == null)
            {
                metadataConnection = new MetadataConnection();
            }
            metadataConnection.doRequestIfNeeded();
        }
        else
        {
            // apply route header filtering later during first subscribe
            routeHeadersByTopic.computeIfAbsent(topicName, t -> new ArrayList<ListFW<KafkaHeaderFW>>())
                .add(routeHeaders);
        }
    }

    private void addRoute(
        String topicName,
        ListFW<KafkaHeaderFW> routeHeaders,
        boolean proactive,
        BiConsumer<KafkaError, String> onMetadataError,
        TopicMetadata topicMetadata)
    {
        KafkaError errorCode = topicMetadata.errorCode();
        switch(errorCode)
        {
        case INVALID_TOPIC_EXCEPTION:
        case UNKNOWN_TOPIC_OR_PARTITION:
            onMetadataError.accept(errorCode, topicName);
            break;
        case NONE:
            if (topicMetadata.compacted)
            {
                NetworkTopic topic = topicsByName.computeIfAbsent(topicName,
                        name -> new NetworkTopic(name, topicMetadata.partitionCount(), topicMetadata.compacted,
                                proactive, topicMetadata.deleteRetentionMs, forceProactiveMessageCache));
                topic.addRoute(routeHeaders);
                doConnections(topicMetadata);
                doFlush();
            }
            break;
        default:
            throw new RuntimeException(format("Unexpected errorCode %d from metadata query for topic %s",
                    errorCode, topicName));
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
        String topicName,
        int attachId,
        Long2LongHashMap fetchOffsets)
    {
        final Consumer<Long2LongHashMap> detacher = detachersById.remove(attachId);
        if (detacher == null)
        {
            TopicMetadata metadata = topicMetadataByName.get(topicName);
            if (metadata != null)
            {
                metadata.doDetach(attachId);
                if (!metadata.hasConsumers())
                {
                    topicMetadataByName.remove(topicName);
                }
            }
        }
        else
        {
            detacher.accept(fetchOffsets);
        }
    }

    void removeConnection(LiveFetchConnection connection)
    {
        connections = ArrayUtil.remove(connections, connection);
    }

    void removeConnection(HistoricalFetchConnection connection)
    {
        historicalConnections = ArrayUtil.remove(historicalConnections, connection);
    }

    abstract class AbstractNetworkConnection
    {
        boolean tempPostBegin;
        final MessageConsumer networkTarget;
        Timer timer;

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
            timer = clientStreamFactory.scheduler.newBlankTimer();
        }

        @Override
        public String toString()
        {
            return format("%s [budget=%d, padding=%d, networkId=%d, networkReplyId=%d, nextRequestId %d, nextResponseId %d]",
                    getClass().getSimpleName(), networkRequestBudget, networkRequestPadding,
                    networkId, networkReplyId, nextRequestId, nextResponseId);
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
            if (networkId == 0L && networkReplyId == 0L)
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
                System.out.println(format("%s: Begin issued", this));
                tempPostBegin = true;
            }
        }

        abstract void doRequestIfNeeded();

        final void metadataRequestIdle()
        {
            routeCounters.metadataRequestIdleTimeouts.getAsLong();
            idle();
        }

        final void describeConfigsRequestIdle()
        {
            routeCounters.describeConfigsRequestIdleTimeouts.getAsLong();
            idle();
        }

        final void fetchRequestIdle()
        {
            routeCounters.fetchRequestIdleTimeouts.getAsLong();
            idle();
        }

        final void listOffsetsRequestIdle()
        {
            routeCounters.listOffsetsRequestIdleTimeouts.getAsLong();
            idle();
        }

        final void idle()
        {
            abort();
            handleConnectionFailed();
        }

        final void abort()
        {
            if (networkId != 0L)
            {
                clientStreamFactory.doAbort(networkTarget, networkId);
                this.networkId = 0L;
                this.networkRequestBudget = 0;
            }
        }

        final void close()
        {
            if (networkId != 0L)
            {
                clientStreamFactory.doEnd(networkTarget, networkId, null);
                this.networkId = 0L;
                this.streamState = this::afterClose;
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
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = clientStreamFactory.resetRO.wrap(buffer, index, index + length);
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
            System.out.println(format("%s: handleReset", this));
            clientStreamFactory.correlations.remove(
                    networkCorrelationId, AbstractNetworkConnection.this);
            if (networkReplyId != 0L)
            {
                clientStreamFactory.doReset(networkReplyThrottle, networkReplyId);
                networkReplyId = 0L;
            }
            handleConnectionFailed();
            timer.cancel();
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
                final BeginFW begin = clientStreamFactory.beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                clientStreamFactory.doReset(networkReplyThrottle, networkReplyId);
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

        private void afterClose(
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
            case AbortFW.TYPE_ID:
                doReinitialize();
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
            timer.cancel();
            clientStreamFactory.scheduler.rescheduleTimeout(readIdleTimeout, timer);

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

                    ResponseHeaderFW response = responseRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                    if (response == null || response.limit() + response.size() > networkLimit)
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
                        networkOffset = response.limit();
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

        abstract void handleConnectionFailed();

        private void handleEnd(
            EndFW end)
        {
            System.out.println(format("%s: handleEnd", this));
            if (networkId != 0L)
            {
                clientStreamFactory.doEnd(networkTarget, networkId, null);
                this.networkId = 0L;
            }
            doReinitialize();
            doRequestIfNeeded();
            timer.cancel();
        }

        private void handleAbort(
            AbortFW abort)
        {
            System.out.println(format("%s: handleAbort", this));
            if (networkId != 0L)
            {
                clientStreamFactory.doAbort(networkTarget, networkId);
                this.networkId = 0L;
            }
            handleConnectionFailed();
            timer.cancel();
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
            networkReplyId = 0L;
            nextRequestId = 0;
            nextResponseId = 0;
        }
    }

    abstract class AbstractFetchConnection extends AbstractNetworkConnection
    {
        private static final long EARLIEST_AVAILABLE_OFFSET = -2L;
        private static final long NEXT_OFFSET = -1L; // high water mark (offset of the next published message)

        final String host;
        final int port;
        final int brokerId;

        int encodeLimit;
        boolean offsetsNeeded;
        boolean offsetsRequested;
        Map<String, long[]> requestedFetchOffsetsByTopic = new HashMap<>();
        final ResponseDecoder fetchResponseDecoder;

        private final LongSupplier fetches;

        private AbstractFetchConnection(
            BrokerMetadata broker,
            LongSupplier fetches)
        {
            super();
            this.brokerId = broker.nodeId;
            this.host = broker.host;
            this.port = broker.port;
            this.fetches = fetches;
            fetchResponseDecoder = new FetchResponseDecoder(
                    this::getTopicDispatcher,
                    this::getRequestedOffset,
                    this::handlePartitionResponseError,
                    localDecodeBuffer);
        }

        @Override
        void doRequestIfNeeded()
        {
            if (this instanceof HistoricalFetchConnection)
            {
                System.out.format("[doRequestIfNeeded()] %s\n", this);
            }

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
                    if (this instanceof HistoricalFetchConnection)
                    {
                        System.out.println("[doRequestIfNeeded()] networkRequestBudget > networkRequestPadding");
                    }

                    if (offsetsNeeded)
                    {
                        doListOffsetsRequest();
                        offsetsRequested = true;
                    }
                    else
                    {
                        doFetchRequest();
                        if (offsetsNeeded)
                        {
                            // Make sure we continue if no fetch request was done because all needed high water mark offset
                            doRequestIfNeeded();
                        }
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
                int partitionCount = addTopicToRequest(topicName,
                        (p, o) -> requestedOffsets[p] = o,
                        (p) -> requestedOffsets[p]);

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
                    System.out.println(format("%s: no fetch needed for topic %s",
                            this, topicsByName.get(topicName)));
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

                fetches.getAsLong();

                NetworkConnectionPool.this.clientStreamFactory.doData(networkTarget, networkId,
                        networkRequestPadding, payload);
                networkRequestBudget -= payload.sizeof() + networkRequestPadding;

                if (tempPostBegin)
                {
                    System.out.println(format("%s: first request (fetch) issued", this));
                    tempPostBegin = false;
                }
                timer.cancel();
                clientStreamFactory.scheduler.rescheduleTimeout(readIdleTimeout, timer, this::fetchRequestIdle);
            }
            else if (topicCount > 0)
            {
                System.out.println(format("fetch request %d bytes BLOCKED, insufficient window %d, padding=%d",
                        encodeLimit - encodeOffset, networkRequestBudget, networkRequestPadding));
            }
            else
            {
                System.out.println(format("%s: historical fetch request abandoned, no topics to fetch, topicCount=%d, topic=%s",
                        this, topicCount,
                        topicsByName.keySet().iterator().hasNext() ? topicsByName.values().iterator().next() : null));
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
                int partitionCount = topicMetadata.offsetsRequired(brokerId);
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
                        if (topicMetadata.nodeIdsByPartition[partitionId] == brokerId &&
                            topicMetadata.offsetsOutOfRangeByPartition[partitionId] != NO_OFFSET)
                        {
                            long requestedTimestamp = topicMetadata.offsetsOutOfRangeByPartition[partitionId] == MAX_OFFSET
                                    ? NEXT_OFFSET
                                    : EARLIEST_AVAILABLE_OFFSET;
                            ListOffsetsPartitionRequestFW listOffsetsPartitionRequest = listOffsetsPartitionRequestRW.wrap(
                                    NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                    NetworkConnectionPool.this.encodeBuffer.capacity())
                                    .partitionId(partitionId)
                                    .timestamp(requestedTimestamp)
                                    .build();

                            encodeLimit = listOffsetsPartitionRequest.limit();
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

                if (tempPostBegin)
                {
                    System.out.println(format("%s: first request listOffsets issued", this));
                    tempPostBegin = false;
                }

                timer.cancel();
                clientStreamFactory.scheduler.rescheduleTimeout(readIdleTimeout, timer, this::listOffsetsRequestIdle);
            }
            else
            {
                System.out.println(format("%s: listOffsets request %d bytes BLOCKED, insufficient window %d, padding=%d",
                        this, encodeLimit - encodeOffset, networkRequestBudget, networkRequestPadding));
            }
        }

        abstract int addTopicToRequest(
            String topicName,
            IntLongConsumer setRequestedOffset,
            IntToLongFunction getRequestedOffset);

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
                timer.cancel();
                clientStreamFactory.scheduler.rescheduleTimeout(readIdleTimeout, timer);

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
                    timer.cancel();
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
            timer.cancel();

            final ListOffsetsResponseFW response = listOffsetsResponseRO.tryWrap(networkBuffer, networkOffset, networkLimit);
            if (response == null)
            {
                abort();
                return;
            }

            int topicCount = response.topicCount();
            networkOffset = response.limit();

            KafkaError errorCode = NONE;
            for (int topicIndex = 0; topicIndex < topicCount && errorCode == NONE; topicIndex++)
            {
                final ListOffsetsTopicFW topic = listOffsetsTopicRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                if (topic == null)
                {
                    abort();
                    return;
                }

                final String16FW name = topic.name();
                String topicName = name.asString();
                TopicMetadata topicMetadata = topicMetadataByName.get(topicName);
                networkOffset = topic.limit();

                final int partitionCount = topic.partitionCount();

                for (int i = 0; i < partitionCount && errorCode == NONE; i++)
                {
                    final ListOffsetsPartitionResponseFW partition =
                            listOffsetsPartitionResponseRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                    if (partition == null)
                    {
                        abort();
                        return;
                    }

                    errorCode = asKafkaError(partition.errorCode());
                    if (errorCode != NONE)
                    {
                        handlePartitionResponseError(name.asString(), i, errorCode);
                        break;
                    }

                    final long offset = partition.firstOffset();
                    final int partitionId = partition.partitionId();

                    long outOfRangeOffset = topicMetadata.offsetsOutOfRangeByPartition[partitionId];
                    if (offset > outOfRangeOffset)
                    {
                        // offset was too low, adjust higher and leave offsetsOutOfRangeByPartition set
                        // for use in ensureOffsetInRange
                        topicMetadata.setFirstOffset(partitionId, offset);
                    }
                    else
                    {
                        NetworkTopic networkTopic = topicsByName.get(topicName);

                        // networkTopic can be null if all clients have detached (e.g.
                        // because we called detach while processing a previous partition response)
                        if (networkTopic != null)
                        {
                            if (outOfRangeOffset == MAX_OFFSET)
                            {
                                // subscribed to streaming topic, offset is high watermark
                                networkTopic.dispatcher.adjustOffset(partitionId, MAX_OFFSET, offset);
                                networkTopic.setLiveOffset(partitionId, offset);
                            }
                            else
                            {
                                // first offset is now lower than client requested offset, topic was
                                // recreated, force client(s) to re-attach at offset 0
                                networkTopic.dispatcher.detach(true);
                            }
                            topicMetadata.offsetsOutOfRangeByPartition[partitionId] = NO_OFFSET;
                        }
                    }

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

        @Override
        void handleConnectionFailed()
        {
            invalidateConnectionMetadata();
            removeConnection();
            metadataConnection.doRequestIfNeeded();
        }

        abstract void removeConnection();

        private void invalidateConnectionMetadata()
        {
            for (TopicMetadata metadata : topicMetadataByName.values())
            {
                if (metadata.invalidateBroker(brokerId))
                {
                    int newAttachId = nextAttachId++;
                    metadata.doAttach(newAttachId, this::metadataUpdated);
                }
            }
        }

        private void metadataUpdated(TopicMetadata metadata)
        {
            doConnections(metadata);
            doFlush();
        }

        private DecoderMessageDispatcher getTopicDispatcher(String topicName)
        {
            NetworkTopic topic = topicsByName.get(topicName);

            // topic may be null if everyone has unsubscribed
            return topic == null ? NOOP_DISPATCHER : topic.dispatcher;
        }

        private void handlePartitionResponseError(
            String topicName,
            int partition,
            KafkaError errorCode)
        {
            switch(errorCode)
            {
            case OFFSET_OUT_OF_RANGE:
                offsetsNeeded = true;
                TopicMetadata topicMetadata = topicMetadataByName.get(topicName);
                long badOffset = getRequestedOffset(topicName, partition);

                // logStartOffset is always -1 in this case so we can't use it
                topicMetadata.setOffsetOutOfRange(partition, badOffset);

                break;
            case UNKNOWN_TOPIC_OR_PARTITION:
            case LEADER_NOT_AVAILABLE:
            case NOT_LEADER_FOR_PARTITION:
                TopicMetadata metadata = topicMetadataByName.get(topicName);
                metadata.setErrorCode(errorCode);
                metadata.scheduleRefresh(
                        clientStreamFactory.scheduler,
                        metadataBackoffMillis,
                        metadataConnection);
                System.out.format(
                        "Fetch failed for topic \"%s\" partition %d due to Kafka error code %s, retrying...\n",
                        topicName, partition, errorCode);
                break;
            default:
                // kafka error, trigger connection failed and reconnect
                abort();
                break;
            }
        }

        @Override
        public String toString()
        {
            return format("%s [brokerId=%d, host=%s, port=%d, budget=%d, padding=%d, networkId=%d, networkReplyId=%d," +
                          "nextRequestId=%d, nextResponseId=%d]",
                    getClass().getSimpleName(), brokerId, host, port, networkRequestBudget, networkRequestPadding,
                    networkId, networkReplyId, nextRequestId, nextResponseId);
        }
    }

    private final class LiveFetchConnection extends AbstractFetchConnection
    {
        LiveFetchConnection(BrokerMetadata broker)
        {
            super(broker, NO_COUNTER);
        }

        @Override
        int addTopicToRequest(
            String topicName,
            IntLongConsumer setRequestedOffset,
            IntToLongFunction getRequestedOffset)
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
                        if (candidate.offset == MAX_OFFSET)
                        {
                            // Trigger list offsets request to get the high water mark offset
                            offsetsNeeded = true;
                            metadata.offsetsOutOfRangeByPartition[candidate.id] = MAX_OFFSET;
                        }
                        else
                        {
                            long offset = metadata.ensureOffsetInRange(candidate.id, candidate.offset);
                            PartitionRequestFW partitionRequest = NetworkConnectionPool.this.partitionRequestRW
                                    .wrap(NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                            NetworkConnectionPool.this.encodeBuffer.capacity())
                                    .partitionId(candidate.id).fetchOffset(offset).maxBytes(maxPartitionBytes).build();

                            long requestedOffset = candidate.offset;

                            if (offset < candidate.offset)
                            {
                                // Topic was recreated, we have to go back to an earlier offset
                                topic.dispatcher.adjustOffset(candidate.id, candidate.offset, offset);
                                requestedOffset = offset;

                                // Prepare to update the partition offset later
                                NetworkTopicPartition partition = candidate.clone();
                                partitionsWorkList.add(partition);
                                offsetsWorkList.addLong(offset);
                            }

                            setRequestedOffset.accept(candidate.id, requestedOffset);
                            encodeLimit = partitionRequest.limit();
                            partitionCount++;
                        }
                    }

                    candidate = next;
                }

                if (!partitionsWorkList.isEmpty())
                {
                    // Update the partition offsets. We must remove and add to preserve ordering.
                    for (int i=0; i < partitionsWorkList.size(); i++)
                    {
                        NetworkTopicPartition partition = partitionsWorkList.get(i);
                        boolean removed = topic.partitions.remove(partition);
                        assert removed;
                        partition.offset = offsetsWorkList.getLong(i);
                        topic.partitions.add(partition);
                    }

                    partitionsWorkList.clear();
                    offsetsWorkList.clear();
                }
            }

            return partitionCount;
        }

        @Override
        void removeConnection()
        {
            NetworkConnectionPool.this.removeConnection(this);
        }
    }

    private final class HistoricalFetchConnection extends AbstractFetchConnection
    {
        private HistoricalFetchConnection(BrokerMetadata broker)
        {
            super(broker, routeCounters.historicalFetches);
        }

        @Override
        int addTopicToRequest(
            String topicName,
            IntLongConsumer setRequestedOffset,
            IntToLongFunction getRequestedOffset)
        {
            int partitionCount = 0;
            int originalEncodeLimit = encodeLimit;

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
                    Iterator<NetworkTopicPartition> partitions = topic.partitions.iterator();

                    while (partitions.hasNext())
                    {
                        NetworkTopicPartition partition = partitions.next();

                        if (topic.needsHistorical(partition.id) &&
                                partitionId < partition.id && nodeIdsByPartition[partition.id] == brokerId)
                        {
                            long offset = metadata.ensureOffsetInRange(partition.id, partition.offset);
                            PartitionRequestFW partitionRequest = NetworkConnectionPool.this.partitionRequestRW
                                .wrap(NetworkConnectionPool.this.encodeBuffer, encodeLimit,
                                        NetworkConnectionPool.this.encodeBuffer.capacity())
                                .partitionId(partition.id)
                                .fetchOffset(offset)
                                .maxBytes(maxPartitionBytes)
                                .build();

                            if (offset < partition.offset)
                            {
                                // Topic was recreated, we have to go back to an earlier offset
                                // We must remove and add the partition to preserve ordering
                                topic.dispatcher.adjustOffset(partition.id, partition.offset, offset);
                                partitions.remove();
                                partition.offset = offset;
                                partitionsWorkList.add(partition);
                            }
                            System.out.println(format("%s addTopicToRequest: requestOffset=%d, topic=%s",
                                    this, partition.offset, topic));
                            setRequestedOffset.accept(partition.id,  partition.offset);
                            encodeLimit = partitionRequest.limit();
                            partitionId = partition.id;
                            partitionCount++;
                        }
                    }

                    if (!partitionsWorkList.isEmpty())
                    {
                        topic.partitions.addAll(partitionsWorkList);
                        partitionsWorkList.clear();
                    }

                    partitionCount = topic.satisfyPartitionRequestsFromCache(
                            partitionCount,
                            getRequestedOffset,
                            setRequestedOffset,
                            NetworkConnectionPool.this.encodeBuffer,
                            originalEncodeLimit,
                            encodeLimit,
                            l -> encodeLimit = l);
                }

            }

            return partitionCount;
        }

        @Override
        void removeConnection()
        {
            NetworkConnectionPool.this.removeConnection(this);
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
                      .filter(m -> !m.isComplete()).findFirst()).isPresent()))
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
                        .configNamesCount(2)
                        .build();

                encodeLimit = resourceRequest.limit();

                String16FW configName = configNameRW.wrap(
                        buffer, encodeLimit, buffer.capacity())
                        .set(CLEANUP_POLICY, UTF_8)
                        .build();

                encodeLimit = configName.limit();

                configName = configNameRW.wrap(
                        buffer, encodeLimit, buffer.capacity())
                        .set(DELETE_RETENTION_MS, UTF_8)
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

                    timer.cancel();
                    clientStreamFactory.scheduler.rescheduleTimeout(readIdleTimeout, timer, this::describeConfigsRequestIdle);
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

                    timer.cancel();
                    clientStreamFactory.scheduler.rescheduleTimeout(readIdleTimeout, timer, this::metadataRequestIdle);
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

        private void handleDescribeConfigsResponse(
            long networkTraceId,
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
            timer.cancel();

            final DescribeConfigsResponseFW describeConfigsResponse =
                    describeConfigsResponseRO.tryWrap(networkBuffer, networkOffset, networkLimit);

            KafkaError error = NONE;
            boolean compacted = false;
            int deleteRetentionMs = KAFKA_SERVER_DEFAULT_DELETE_RETENTION_MS;

            if (describeConfigsResponse == null)
            {
                error = UNEXPECTED_SERVER_ERROR;
            }
            else
            {
                int resourceCount = describeConfigsResponse.resourceCount();
                networkOffset = describeConfigsResponse.limit();

                loop:
                for (int resourceIndex = 0; resourceIndex < resourceCount && error == NONE; resourceIndex++)
                {
                    final ResourceResponseFW resource = resourceResponseRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                    if (resource == null)
                    {
                        error = UNEXPECTED_SERVER_ERROR;
                        break loop;
                    }

                    final String16FW name = resource.name();
                    assert name.asString().equals(pendingTopicMetadata.topicName);
                    error = asKafkaError(resource.errorCode());
                    if (error != NONE)
                    {
                        break;
                    }
                    final int configCount = resource.configCount();
                    networkOffset = resource.limit();

                    for (int configIndex = 0; configIndex < configCount && error == NONE; configIndex++)
                    {
                        final ConfigResponseFW configResponse =
                                configResponseRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                        if (configResponse == null)
                        {
                            error = UNEXPECTED_SERVER_ERROR;
                            break loop;
                        }

                        String configName = configResponse.name().asString();
                        switch (configName)
                        {
                        case CLEANUP_POLICY:
                            compacted = configResponse.value() != null && configResponse.value().asString().equals(COMPACT);
                            networkOffset = configResponse.limit();
                            break;
                        case DELETE_RETENTION_MS:
                            deleteRetentionMs = configResponse.value() == null ? KAFKA_SERVER_DEFAULT_DELETE_RETENTION_MS :
                                parseInt(configResponse.value().asString());
                            break;
                        default:
                            assert false : format("Unexpected config name %s in describe configs response", configName);
                        }
                    }
                }
            }

            if (error.isRecoverable())
            {
                pendingTopicMetadata.scheduleRefresh(
                        clientStreamFactory.scheduler,
                        metadataBackoffMillis,
                        metadataConnection);
                pendingTopicMetadata = null;
            }
            else if (error != NONE)
            {
                // kafka error, trigger connection failed and reconnect
                pendingTopicMetadata.invalidate();
                abort();
            }
            else
            {
                // Guard against a possible race with invalidateBroker called due to fetch connection failure
                if (pendingTopicMetadata.nextRequiredRequestType == MetadataRequestType.DESCRIBE_CONFIGS)
                {
                    TopicMetadata metadata = pendingTopicMetadata;
                    pendingTopicMetadata = null;
                    metadata.setCompacted(compacted);
                    metadata.setDeleteRetentionMs(deleteRetentionMs);
                    KafkaError priorError = metadata.setComplete(error);
                    metadata.flush();
                    switch (priorError)
                    {
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        // Topic was re-created, force clients to re-attach at offset zero
                        NetworkTopic topic = topicsByName.get(metadata.topicName);
                        if (topic != null)
                        {
                            topic.dispatcher.detach(true);
                        }
                        break;
                    default:
                        break;
                    }
                }
            }
        }

        private void handleMetadataResponse(
            long networkTraceId,
            DirectBuffer networkBuffer,
            int networkOffset,
            final int networkLimit)
        {
            timer.cancel();

            KafkaError error = decodeMetadataResponse(networkBuffer, networkOffset, networkLimit);
            final TopicMetadata topicMetadata = pendingTopicMetadata;
            switch(error)
            {
            case NONE:
                topicMetadata.nextRequiredRequestType = MetadataRequestType.DESCRIBE_CONFIGS;
                if (topicMetadata.retries > 0)
                {
                    System.out.format(
                            "Metadata for topic \"%s\" has now been found\n",
                            topicMetadata.topicName);
                }
                break;
            case LEADER_NOT_AVAILABLE:
            case TOPIC_AUTHORIZATION_FAILED:
            case UNKNOWN_TOPIC_OR_PARTITION:
                if (topicMetadata.hasConsumers())
                {
                    if (topicMetadata.retries == 0)
                    {
                        System.out.format(
                                "Unable to access metadata for topic \"%s\" due to Kafka error code %s, retrying...\n",
                                topicMetadata.topicName, error);
                    }
                    topicMetadata.scheduleRefresh(
                            clientStreamFactory.scheduler,
                            metadataBackoffMillis,
                            metadataConnection);
                }
                pendingTopicMetadata = null;
                break;
            case INVALID_TOPIC_EXCEPTION:
            case PARTITION_COUNT_CHANGED:
                String topicName = topicMetadata.topicName;
                topicMetadataByName.remove(topicName);
                detachSubscribers(topicName, false);
                topicsByName.remove(topicName);
                pendingTopicMetadata = null;
                topicMetadata.setErrorCode(error);
                topicMetadata.flush();
                break;
            default:
                // internal Kafka error, trigger connection failed and reconnect
                topicMetadata.invalidate();
                abort();
                break;
            }
        }

        private KafkaError decodeMetadataResponse(
            DirectBuffer networkBuffer,
            int networkOffset,
            int networkLimit)
        {
            final MetadataResponseFW metadataResponse = metadataResponseRO.tryWrap(networkBuffer, networkOffset, networkLimit);
            if (metadataResponse == null)
            {
                return UNEXPECTED_SERVER_ERROR;
            }

            final int brokerCount = metadataResponse.brokerCount();
            pendingTopicMetadata.initializeBrokers(brokerCount);
            networkOffset = metadataResponse.limit();
            KafkaError error = NONE;
            for (int brokerIndex=0; brokerIndex < brokerCount; brokerIndex++)
            {
                final BrokerMetadataFW broker = brokerMetadataRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                if (broker == null)
                {
                    return UNEXPECTED_SERVER_ERROR;
                }

                pendingTopicMetadata.addBroker(broker.nodeId(), broker.host().asString(), broker.port());
                networkOffset = broker.limit();
            }

            final MetadataResponsePart2FW metadataResponsePart2 =
                    metadataResponsePart2RO.tryWrap(networkBuffer, networkOffset, networkLimit);
            if (metadataResponsePart2 == null)
            {
                return UNEXPECTED_SERVER_ERROR;
            }

            final int topicCount = metadataResponsePart2.topicCount();
            assert topicCount == 1;
            networkOffset = metadataResponsePart2.limit();
            for (int topicIndex = 0; topicIndex < topicCount && error == NONE; topicIndex++)
            {
                final TopicMetadataFW topicMetadata = topicMetadataRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                if (topicMetadata == null)
                {
                    return UNEXPECTED_SERVER_ERROR;
                }

                String16FW topicName = topicMetadata.topic();
                assert topicName.asString().equals(pendingTopicMetadata.topicName);
                error = asKafkaError(topicMetadata.errorCode());
                if (error != NONE)
                {
                    break;
                }
                final int partitionCount = topicMetadata.partitionCount();
                networkOffset = topicMetadata.limit();

                if (pendingTopicMetadata.initializePartitions(partitionCount))
                {
                    for (int partitionIndex = 0; partitionIndex < partitionCount && error == NONE; partitionIndex++)
                    {
                        final PartitionMetadataFW partition =
                                partitionMetadataRO.tryWrap(networkBuffer, networkOffset, networkLimit);
                        if (partition == null)
                        {
                            return UNEXPECTED_SERVER_ERROR;
                        }

                        error = KafkaError.asKafkaError(partition.errorCode());
                        pendingTopicMetadata.addPartition(partition.partitionId(), partition.leader());
                        networkOffset = partition.limit();
                    }
                }
                else
                {
                    error = KafkaError.PARTITION_COUNT_CHANGED;
                }
            }

            return error;
        }

        private void detachSubscribers(
            String topicName,
            boolean reattach)
        {
            NetworkTopic topic = topicsByName.get(topicName);
            if (topic != null)
            {
                topic.dispatcher.detach(reattach);
            }
        }

        @Override
        void handleConnectionFailed()
        {
            doReinitialize();
            doRequestIfNeeded();
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
        private BitSet isLiveByPartition = new BitSet();
        private final boolean proactive;

        @Override
        public String toString()
        {
            return format("topicName=%s, partitions=%s, needsHistoricalByPartition=%s, isLiveByPartition=%s",
                    topicName, partitions, needsHistoricalByPartition, isLiveByPartition);
        }

        private NetworkTopic(
            String topicName,
            int partitionCount,
            boolean compacted,
            boolean proactive,
            int deleteRetentionMs,
            boolean forceProactiveMessageCache)
        {
            this.topicName = topicName;
            this.compacted = compacted;
            this.proactive = proactive;
            this.windowSuppliers = new HashSet<>();
            this.partitions = new TreeSet<>();
            this.candidate = new NetworkTopicPartition();
            this.progressHandler = this::handleProgress;
            PartitionIndex[] partitionIndexes = new PartitionIndex[partitionCount];

            if (compacted)
            {
                for (int i = 0; i < partitionCount; i++)
                {
                    partitionIndexes[i] = new CompactedPartitionIndex(1000, deleteRetentionMs,
                            messageCache);
                }
            }
            else
            {
                for (int i = 0; i < partitionCount; i++)
                {
                    partitionIndexes[i] = DEFAULT_PARTITION_INDEX;
                }
            }

            this.dispatcher = new TopicMessageDispatcher(
                    partitionIndexes,
                    compacted ? CompactedHeaderValueMessageDispatcher::new : HeaderValueMessageDispatcher::new);

            // Cache only messages matching route header conditions
            List<ListFW<KafkaHeaderFW>> routeHeadersList = routeHeadersByTopic.remove(topicName);

            if (routeHeadersList != null)
            {
                routeHeadersList.forEach(this::addRoute);
            }

            if (proactive)
            {
                 for (int i=0; i < partitionCount; i++)
                 {
                     attachToPartition(i, 0L, 1);
                 }
                 MessageDispatcher bootstrapDispatcher = new ProgressUpdatingMessageDispatcher(partitionCount, progressHandler);
                 this.dispatcher.add(null, -1, Collections.emptyIterator(), bootstrapDispatcher);
            }
        }

        void addRoute(
            ListFW<KafkaHeaderFW> routeHeaders)
        {
            // Cache only messages matching route conditions
            if (routeHeaders != null && !routeHeaders.isEmpty())
            {
                this.dispatcher.add(null, -1, headersIterator.wrap(routeHeaders), MATCHING_MESSAGE_DISPATCHER);
                dispatcher.enableProactiveMessageCaching();
            }
            else
            {
                this.dispatcher.add(null, -1, emptyIterator(), MATCHING_MESSAGE_DISPATCHER);
                if (forceProactiveMessageCache)
                {
                    dispatcher.enableProactiveMessageCaching();
                }
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
            headersIterator.wrap(headers);

            if (fetchKey == null)
            {
                this.dispatcher.add(null, -1, headersIterator, dispatcher);
                final LongIterator keys = fetchOffsets.keySet().iterator();
                while (keys.hasNext())
                {
                    final int partitionId = (int) keys.nextValue();
                    long fetchOffset = fetchOffsets.get(partitionId);
                    long cachedOffset = this.dispatcher.entries(partitionId, fetchOffset).next().offset();
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
                int fetchKeyPartition = fetchOffsets.keySet().iterator().next().intValue();
                this.dispatcher.add(fetchKey, fetchKeyPartition, headersIterator, dispatcher);
                long fetchOffset = fetchOffsets.get(fetchKeyPartition);
                if (compacted)
                {
                    long cachedOffset = this.dispatcher.getEntry(fetchKeyPartition, fetchOffset, fetchKey).offset();
                    if (cachedOffset > fetchOffset)
                    {
                        fetchOffsets.put(fetchKeyPartition, cachedOffset);
                        fetchOffset = cachedOffset;
                    }
                }
                attachToPartition(fetchKeyPartition, fetchOffset, 1);
            }

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

            if (fetchOffset == MAX_OFFSET &&
                partition != null && partition.id == candidate.id && isLiveByPartition.get(partitionId))
            {
                // Attach to live stream
                candidate.offset = partition.offset;
                dispatcher.adjustOffset(partitionId, fetchOffset, partition.offset);
            }

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
                add(partition);

                if (fetchOffset == MAX_OFFSET)
                {
                    isLiveByPartition.set(partitionId);
                }
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
            headersIterator.wrap(headers);
            this.dispatcher.remove(fetchKey, fetchKeyPartition, headersIterator, dispatcher);
            final LongIterator partitionIds = fetchOffsets.keySet().iterator();
            while (partitionIds.hasNext())
            {
                long partitionId = partitionIds.nextValue();
                doDetach((int) partitionId, fetchOffsets.get(partitionId), supplyWindow);
            }
            if (partitions.isEmpty()  && !compacted)
            {
                topicsByName.remove(topicName);
                TopicMetadata metadata = topicMetadataByName.get(topicName);
                if (metadata != null && metadata.complete && metadata.consumers.isEmpty())
                {
                    topicMetadataByName.remove(topicName);
                }
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
            if (partition == null || partition.id != candidate.id || partition.offset != candidate.offset)
            {
                throw new IllegalStateException(
                   format("floor gave %s, expected (id=%d, offset=%d); topic=%s",
                           partition, partitionId, fetchOffset, this));
            }

            partition.refs--;
            if (partition.refs == 0)
            {
                remove(partition);

                if (isLiveByPartition.get(partitionId))
                {
                    // If we just removed the highest offset then we are no longer on live stream
                    partition = partitions.floor(candidate);
                    if (partition != null && partition.id == partitionId && partition.offset < fetchOffset)
                    {
                        isLiveByPartition.clear(partitionId);
                    }
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
            }
            return writableBytes;
        }

        int satisfyPartitionRequestsFromCache(
            final int partitionCount,
            IntToLongFunction getRequestedOffset,
            IntLongConsumer setRequestedOffset,
            MutableDirectBuffer encodeBuffer,
            int encodeOffset,
            final int encodeLimit,
            IntConsumer setNewLimit)
        {
            int newPartitionCount = partitionCount;
            int newEncodeLimit = encodeLimit;
            for (int i=0; i < partitionCount; i++)
            {
                PartitionRequestFW request = partitionRequestRO.wrap(encodeBuffer, encodeOffset, encodeLimit);
                assert request.limit() <= encodeLimit;
                int partitionId = request.partitionId();
                final long fetchOffset = request.fetchOffset();
                long logStartOffset = request.logStartOffset();
                int maxBytes = request.maxBytes();
                long newOffset = fetchOffset;
                Iterator<Entry> entries = dispatcher.entries(partitionId, fetchOffset);
                boolean partitionRequestNeeded = false;
                boolean flushNeeded = false;
                final long requestOffset = getRequestedOffset.applyAsLong(partitionId);

                while(entries.hasNext())
                {
                    Entry entry = entries.next();
                    newOffset = entry.offset();
                    MessageFW message = messageCache.get(entry.message(), messageRO);
                    if (message == null)
                    {
                        partitionRequestNeeded = true;
                        System.out.format("[satisfyPartitionRequestsFromCache()] cache miss for partition=%d offset=%d\n",
                                partitionId, entry.offset());
                        break;
                    }
                    else
                    {
                        DirectBuffer key = wrap(keyBuffer, message.key());
                        DirectBuffer value = wrap(valueBuffer,  message.value());
                        HeadersFW headers = headersRO.wrap(message.headers().buffer(),  message.headers().offset(),
                                message.headers().limit());

                        // call the dispatch variant which does not attempt to re-cache the message
                        int dispatched = dispatcher.dispatch(partitionId, requestOffset, newOffset, key, headers.headerSupplier(),
                                message.timestamp(), message.traceId(), value);

                        flushNeeded = true;
                        newOffset++;

                        if (MessageDispatcher.blocked(dispatched) && !MessageDispatcher.delivered(dispatched))
                        {
                            // TODO: this may be too conservative, other dispatchers which did not match this message
                            //       might still have available window to deliver later messages
                            System.out.format(
                                    "[satisfyPartitionRequestsFromCache()] blocked delivery on partition=%d offset=%d\n",
                                    partitionId, newOffset);
                            break;
                        }
                    }
                } // end for each partition index entry

                if (!partitionRequestNeeded)
                {
                    clientStreamFactory.counters.cacheHits.getAsLong();

                    if (!entries.hasNext())
                    {
                        //  End of the partition index reached, advance to latest offset
                        newOffset = dispatcher.nextOffset(partitionId);
                    }

                    // Remove the partition request by shifting up the subsequent ones
                    encodeBuffer.putBytes(encodeOffset,  encodeBuffer, request.limit(), encodeLimit);
                    newEncodeLimit -= request.sizeof();
                    newPartitionCount--;
                }
                else
                {
                    clientStreamFactory.counters.cacheMisses.getAsLong();

                    // Update the partition request if needed
                    if (newOffset > fetchOffset)
                    {
                        request = partitionRequestRW.wrap(encodeBuffer, encodeOffset, newEncodeLimit)
                            .partitionId(partitionId)
                            .fetchOffset(newOffset)
                            .logStartOffset(logStartOffset)
                            .maxBytes(maxBytes)
                            .build();
                    }
                    encodeOffset = request.limit();
                }

                if (flushNeeded || newOffset > fetchOffset)
                {
                    dispatcher.flush(partitionId, requestOffset, newOffset);
                    setRequestedOffset.accept(partitionId, newOffset);
                    if (!needsHistorical(partitionId))
                    {
                        // caught up to live stream
                        System.out.format("[satisfyPartitionRequestsFromCache()] caught up to live on partition=%d\n",
                                partitionId);
                    }
                }
            } // end for each partition

            assert encodeLimit <= encodeLimit;
            if (newEncodeLimit < encodeLimit)
            {
                setNewLimit.accept(newEncodeLimit);
            }

            System.out.format("[satisfyPartitionRequestsFromCache()] newPartitionCount=%d\n", newPartitionCount);
            return newPartitionCount;
        }

        private void handleProgress(
            int partitionId,
            long firstOffset,
            long nextOffset)
        {
            candidate.id = partitionId;
            candidate.offset = firstOffset;
            NetworkTopicPartition first = partitions.floor(candidate);

            if (first == null || first.id != partitionId || first.offset != firstOffset)
            {
                throw new IllegalStateException(
                        format("floor gave %s, expected (id=%d, offset=%d); nextOffset = %d, topic=%s",
                                first, partitionId, firstOffset, nextOffset, this));
            }

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
                add(next);
            }
            next.refs++;
            if (first.refs == 0)
            {
                remove(first);
            }
        }

        private void add(
            NetworkTopicPartition partition)
        {
            partitions.add(partition);
//            System.out.println(format("After partitions.add(%s): %s", partition, partitions));
        }

        private void remove(
            NetworkTopicPartition partition)
        {
            partitions.remove(partition);
//            System.out.println(format("After partitions.remove(%s): %s", partition, partitions));
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

        void setLiveOffset(
            int partitionId,
            long offset)
        {
            assert isLiveByPartition.get(partitionId);
            candidate.id = partitionId;
            candidate.offset = MAX_OFFSET;
            NetworkTopicPartition maxOffset = partitions.floor(candidate);
            assert maxOffset.id == partitionId;
            assert maxOffset.offset == MAX_OFFSET;
            partitions.remove(maxOffset);

            NetworkTopicPartition existing = partitions.floor(candidate);

            if (existing != null && existing.id == partitionId && existing.offset == offset)
            {
                existing.refs++;
                NetworkTopicPartition floor = partitions.floor(existing);
                if (floor.id == partitionId && floor.offset != offset)
                {
                    needsHistoricalByPartition.set(partitionId, true);
                }
            }
            else
            {
                NetworkTopicPartition partition = maxOffset.clone();
                partition.offset = offset;
                partitions.add(partition);
                needsHistoricalByPartition.set(partition.id, true);
            }
        }

        DirectBuffer wrap(MutableDirectBuffer wrapper, Flyweight wrapped)
        {
            DirectBuffer result = null;
            if (wrapped != null)
            {
                wrapper.wrap(wrapped.buffer(), wrapped.offset(), wrapped.sizeof());
                result = wrapper;
            }
            return result;
        }
    }

    static final class NetworkTopicPartition implements Comparable<NetworkTopicPartition>
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
        protected NetworkTopicPartition clone()
        {
            NetworkTopicPartition result = new NetworkTopicPartition();
            result.id = id;
            result.offset = offset;
            result.refs = refs;
            return result;
        }

        @Override
        public int compareTo(
            NetworkTopicPartition that)
        {
            int comparison = Integer.compare(this.id, that.id);

            if (comparison == 0)
            {
                comparison = Long.compare(this.offset, that.offset);
            }

            assert compareToResponseValid(that, comparison) : format("compareTo response %d invalid for this=%s, that=%s",
                    comparison, this, that);

            return comparison;
        }

        private boolean compareToResponseValid(
            NetworkTopicPartition that,
            int comparison)
        {
            return (comparison == 0 && this.id - that.id == 0 && this.offset - that.offset == 0L)
                    ||  (comparison < 0 && (this.id < that.id) || (this.offset < that.offset))
                    ||   (comparison > 0 && (this.id > that.id) || (this.offset > that.offset));
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
            return 31 * id + Long.hashCode(offset);
        }

        @Override
        public String toString()
        {
            return format("(id=%d, offset=%d, refs=%d)", id, offset, refs);
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
        private static final int UNKNOWN_BROKER = -1;

        private final String topicName;
        private KafkaError errorCode = NONE;
        private boolean compacted;
        private int deleteRetentionMs;
        private boolean complete;
        BrokerMetadata[] brokers;
        private int nextBrokerIndex;
        private int[] nodeIdsByPartition;
        private long[] firstOffsetsByPartition;
        private long[] offsetsOutOfRangeByPartition;
        private Int2ObjectHashMap<Consumer<TopicMetadata>> consumers = new Int2ObjectHashMap<>();
        private MetadataRequestType nextRequiredRequestType = MetadataRequestType.METADATA;
        private int retries;
        private Timer retryTimer;

        TopicMetadata(String topicName)
        {
            this.topicName = topicName;
        }

        boolean isComplete()
        {
            return complete;
        }

        KafkaError setComplete(
            KafkaError error)
        {
            complete = true;
            KafkaError priorError = errorCode;
            errorCode = error;
            if (retryTimer != null)
            {
                retryTimer.cancel();
            }
            return priorError;
        }

        void scheduleRefresh(
            DelayedTaskScheduler scheduler,
            Backoff backoffMillis,
            MetadataConnection connection)
        {
            if (complete)
            {
                invalidate();
                retries = 0;
            }

            // disable metadata gets until timer expires
            complete = true;

            Timer timer = getOrCreateTimer(scheduler);
            timer.cancel();
            scheduler.rescheduleTimeout(backoffMillis.next(retries), timer, () -> doRefresh(connection));
        }

        void doRefresh(
            MetadataConnection connection)
        {
            complete = false;
            retries++;
            connection.doRequestIfNeeded();
        }

        MetadataRequestType nextRequiredRequestType()
        {
            return nextRequiredRequestType;
        }

        void setCompacted(boolean compacted)
        {
            this.compacted = compacted;
        }

        void setDeleteRetentionMs(
            int deleteRetentionMs)
        {
             this.deleteRetentionMs = deleteRetentionMs;
        }

        void setErrorCode(
            KafkaError error)
        {
            this.errorCode = error;
        }

        void initializeBrokers(int brokerCount)
        {
            brokers = brokers != null && brokers.length == brokerCount ? brokers : new BrokerMetadata[brokerCount];
            nextBrokerIndex = 0;
        }

        void addBroker(int nodeId, String host, int port)
        {
            brokers[nextBrokerIndex++] = new BrokerMetadata(nodeId, host, port);
        }

        boolean invalidateBroker(
            int brokerId)
        {
            boolean brokerRemoved = false;
            if (brokers != null)
            {
                for (int i = 0; i < brokers.length; i++)
                {
                    if (brokers[i] != null && brokers[i].nodeId == brokerId)
                    {
                        brokers[i] = null;
                        complete = false;
                        brokerRemoved = true;
                    }
                }
                if (brokerRemoved  && nodeIdsByPartition != null)
                {
                    nextRequiredRequestType = MetadataRequestType.METADATA;
                    for (int i = 0; i < nodeIdsByPartition.length; i++)
                    {
                        if (nodeIdsByPartition[i] == brokerId)
                        {
                            nodeIdsByPartition[i] = UNKNOWN_BROKER;
                        }
                    }
                }
            }
            return brokerRemoved;
        }

        void invalidate()
        {
            complete = false;
            if (brokers != null)
            {
                Arrays.fill(brokers, null);
            }
            if (nodeIdsByPartition != null)
            {
                Arrays.fill(nodeIdsByPartition, UNKNOWN_BROKER);
            }
            nextRequiredRequestType = MetadataRequestType.METADATA;
        }

        boolean initializePartitions(int partitionCount)
        {
            boolean result = true;
            if (nodeIdsByPartition == null)
            {
                nodeIdsByPartition = new int[partitionCount];
                firstOffsetsByPartition = new long[partitionCount];
                offsetsOutOfRangeByPartition = new long[partitionCount];
                Arrays.fill(offsetsOutOfRangeByPartition, NO_OFFSET);
            }
            else if (nodeIdsByPartition.length != partitionCount)
            {
                result = false;
            }
            return result;
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

        long ensureOffsetInRange(
            int partition,
            long offset)
        {
            long result = offset;
            if (offsetsOutOfRangeByPartition[partition] == offset)
            {
                result = firstOffsetsByPartition[partition];
                offsetsOutOfRangeByPartition[partition] = NO_OFFSET;
            }
            return result;
        }


        int offsetsRequired(
            int nodeId)
        {
            int result = 0;
            if (nodeIdsByPartition != null)
            {
                for (int i=0; i < nodeIdsByPartition.length; i++)
                {
                    if (nodeIdsByPartition[i] == nodeId &&
                            offsetsOutOfRangeByPartition[i] != NO_OFFSET)
                    {
                        result++;
                    }
                }
            }
            return result;
        }

        void setFirstOffset(int partitionId, long offset)
        {
            firstOffsetsByPartition[partitionId] = offset;
        }

        public void setOffsetOutOfRange(
            int partition,
            long badOffset)
        {
            offsetsOutOfRangeByPartition[partition] = badOffset;
        }

        @Override
        public String toString()
        {
            return format(
                "[TopicMetadata] topicName=%s, errorCode=%s, compacted=%b, complete=%b, brokers=%s, nodnodeIdsByPartition=%s",
                topicName, errorCode, compacted, complete, brokers, nodeIdsByPartition);
        }

        void doAttach(
            int consumerId,
            Consumer<TopicMetadata> consumer)
        {
            consumers.put(consumerId, consumer);
            if (complete)
            {
                flush();
            }
        }

        void doDetach(
            int consumerId)
        {
            consumers.remove(consumerId);
            if (retryTimer != null && consumers.isEmpty())
            {
                retryTimer.cancel();
            }
        }

        void flush()
        {
            for (Consumer<TopicMetadata> consumer: consumers.values())
            {
                consumer.accept(this);
            }
            consumers.clear();
        }

        KafkaError errorCode()
        {
            return errorCode;
        }

        boolean hasConsumers()
        {
            return !consumers.isEmpty();
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

        private Timer getOrCreateTimer(
            DelayedTaskScheduler scheduler)
        {
            if (retryTimer == null)
            {
                retryTimer = scheduler.newBlankTimer();
            }
            return retryTimer;
        }
    }

    private static final class ProgressUpdatingMessageDispatcher  implements MessageDispatcher
    {
        private final long[] offsets;
        private final PartitionProgressHandler progressHandler;

        ProgressUpdatingMessageDispatcher(
                int partitions,
                PartitionProgressHandler progressHandler)
        {
            offsets = new long[partitions];
            this.progressHandler = progressHandler;
        }

        @Override
        public void adjustOffset(
            int partition,
            long oldOffset,
            long newOffset)
        {
        }

        @Override
        public void detach(
            boolean reattach)
        {
        }

        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageOffset,
            DirectBuffer key,
            java.util.function.Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            return 0;
        };

        @Override
        public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
        {
            if  (lastOffset > offsets[partition])
            {
                progressHandler.handle(partition, offsets[partition], lastOffset);
                offsets[partition] = lastOffset;
            }
        }
    }

    private static final class KafkaHeadersIterator implements Iterator<KafkaHeaderFW>
    {
        private final KafkaHeaderFW headerRO = new KafkaHeaderFW();
        private final IntArrayList offsets = new IntArrayList();

        private ListFW<KafkaHeaderFW> headers;
        private int position;

        private Iterator<KafkaHeaderFW> wrap(
                ListFW<KafkaHeaderFW> headers)
        {
            this.headers = headers;
            offsets.clear();
            headers.forEach(h -> offsets.addInt(h.offset()));
            position = 0;
            return this;
        }

        @Override
        public boolean hasNext()
        {
            return position < offsets.size();
        }

        @Override
        public KafkaHeaderFW next()
        {
            int currentPosition = position;
            position++;
            return headerRO.wrap(headers.buffer(), offsets.getInt(currentPosition), headers.limit());
        }
    }

}
