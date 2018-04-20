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

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.kafka.internal.stream.ClientStreamFactory.EMPTY_BYTE_ARRAY;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.NONE;

import java.util.function.Function;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.kafka.internal.function.StringIntShortConsumer;
import org.reaktivity.nukleus.kafka.internal.function.StringIntToLongFunction;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.Varint32FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TransactionResponseFW;

public class FetchResponseDecoder
{
    final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    final TopicResponseFW topicResponseRO = new TopicResponseFW();
    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final TransactionResponseFW transactionResponseRO = new TransactionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();
    private final Varint32FW varint32RO = new Varint32FW();

    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final RecordFW recordRO = new RecordFW();
    private final HeaderFW headerRO = new HeaderFW();

    private final Function<String, MessageDispatcher> getDispatcher;
    private final StringIntToLongFunction getRequestedOffsetForPartition;
    private final StringIntShortConsumer errorHandler;
    private final int messageSizeLimit;
    private final BufferPool bufferPool;
    private final long streamId;

    private DecoderState decoderState;
    private int responseSize = -1;
    private int responseBytesProcessed;
    private int slot = NO_SLOT;
    private int slotOffset;
    private int slotLimit;

    private int topicCount;
    private String topicName;
    private MessageDispatcher topicDispatcher;
    private int partitionCount;
    private int recordSetLimit;
    private int recordBatchSize;
    private long highWatermark;
    private int partition;
    private int abortedTransactionCount;
    private long requestedOffset;
    private long nextFetchAt;
    private int recordCount;
    private long firstOffset;
    private int lastOffsetDelta;
    private long firstTimestamp;
    private int recordLimit;
    private final Headers headers = new Headers();
    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final UnsafeBuffer valueBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    FetchResponseDecoder(
        Function<String, MessageDispatcher> getDispatcher,
        StringIntToLongFunction getRequestedOffsetForPartition,
        StringIntShortConsumer errorHandler,
        BufferPool bufferPool,
        int messageSizeLimit,
        long streamId)
    {
        this.getDispatcher = getDispatcher;
        this.getRequestedOffsetForPartition = getRequestedOffsetForPartition;
        this.errorHandler = errorHandler;
        this.bufferPool = requireNonNull(bufferPool);
        this.messageSizeLimit = messageSizeLimit;
        this.streamId = streamId;
        this.decoderState = this::decodeResponseHeader;
    }

    public boolean decode(OctetsFW payload,
                          long traceId)
    {
        boolean responseComplete = false;
        DirectBuffer buffer = payload.buffer();
        int offset = payload.offset();
        int limit = payload.limit();
        if (slot != NO_SLOT)
        {
            buffer = appendToSlot(buffer, offset, limit);
            offset = slotOffset;
            limit = slotLimit;
        }
        int newOffset = decode(buffer, offset, limit, traceId);
        responseBytesProcessed += limit - newOffset;
        if (newOffset == limit && responseBytesProcessed == responseSize)
        {
            responseComplete = true;
            free();
        }
        else
        {
            if (slot == NO_SLOT)
            {
                slot = bufferPool.acquire(streamId);
                slotOffset = 0;
                slotLimit = 0;
                appendToSlot(buffer, newOffset, limit);
            }
        }
        return responseComplete;
    }

    public void free()
    {
        if (slot != NO_SLOT)
        {
            bufferPool.release(slot);
            slot = NO_SLOT;
            slotOffset = 0;
            slotLimit = 0;
            responseSize = -1;
            responseBytesProcessed = 0;
            topicCount = 0;
            partitionCount = 0;
        }
    }

    private DirectBuffer appendToSlot(DirectBuffer buffer, int offset, int limit)
    {
        final MutableDirectBuffer bufferSlot = bufferPool.buffer(slot);
        final int bytesToCopy = limit -  offset;
        bufferSlot.putBytes(slotOffset, buffer, offset, bytesToCopy);
        slotLimit += bytesToCopy;
        return bufferSlot;
    }

    private int decode(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        boolean workWasDone = true;
        while (offset < limit && workWasDone)
        {
            int previousOffset = offset;
            DecoderState previousState = decoderState;
            offset = decoderState.decode(buffer, offset, limit, traceId);
            workWasDone = offset != previousOffset || decoderState != previousState;
        }
        return offset;
    }

    private int decodeResponseHeader(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        ResponseHeaderFW response = responseRO.tryWrap(buffer, offset, limit);
        if (response != null)
        {
            responseSize = response.size();
            decoderState = this::decodeFetchResponse;
            newOffset = response.limit();
        }
        return newOffset;
    }

    private int decodeFetchResponse(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        FetchResponseFW response = fetchResponseRO.tryWrap(buffer, offset, limit);
        if (response != null)
        {
            topicCount = response.topicCount();
            newOffset = response.limit();
        }
        decoderState = this::decodeTopicResponse;
        return newOffset;
    }

    private int decodeTopicResponse(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (topicCount > 0)
        {
            TopicResponseFW response = topicResponseRO.tryWrap(buffer, offset, limit);
            if (response != null)
            {
                topicCount--;
                topicName = response.name().asString();
                topicDispatcher = getDispatcher.apply(topicName);
                partitionCount = response.partitionCount();
                decoderState = this::decodePartitionResponse;
                newOffset = response.limit();
            }
        }
        else
        {
            decoderState = this::decodeResponseHeader;
        }
        return newOffset;
    }

    private int decodePartitionResponse(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (partitionCount > 0)
        {
            PartitionResponseFW response = partitionResponseRO.tryWrap(buffer, offset, limit);
            if (response != null)
            {
                partitionCount--;
                newOffset = response.limit();
                partition = response.partitionId();
                short errorCode = response.errorCode();
                if (errorCode != NONE)
                {
                    errorHandler.accept(topicName, partition, errorCode);
                }
                else
                {
                    highWatermark = response.highWatermark();
                    abortedTransactionCount = response.abortedTransactionCount();
                    decoderState = abortedTransactionCount > 0 ? this::decodeTransactionResponse : this::decodeRecordSet;
                }
            }
        }
        else
        {
            this.decoderState = this::decodeTopicResponse;
        }
        return newOffset;
    }

    private int decodeTransactionResponse(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (abortedTransactionCount > 0)
        {
            TransactionResponseFW response = transactionResponseRO.tryWrap(buffer, offset, limit);
            if (response != null)
            {
                abortedTransactionCount--;
                newOffset = response.limit();
            }
        }
        else
        {
            decoderState = this::decodeRecordSet;
        }
        return newOffset;
    }

    private int decodeRecordSet(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        RecordSetFW response = recordSetRO.tryWrap(buffer, offset, limit);
        if (response != null)
        {
            recordBatchSize = response.recordBatchSize();
            recordSetLimit = response.limit() + recordBatchSize;
            if (recordBatchSize == 0)
            {
                long requestedOffset = getRequestedOffsetForPartition.apply(topicName, partition);
                if (highWatermark > requestedOffset)
                {
                    System.out.format(
                            "[nukleus-kafka] skipping topic: %s partition: %d offset: %d because record batch size is 0\n",
                            topicName, partition, requestedOffset);
                    topicDispatcher.flush(partition, requestedOffset, requestedOffset + 1);
                }
                decoderState = this::decodeSkipRecordSet;
            }
            else
            {
                decoderState = this::decodeRecordBatch;
            }
        }
        return newOffset;
    }

    private int decodeRecordBatch(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        RecordBatchFW recordBatch = recordBatchRO.tryWrap(buffer, offset, limit);
        if (recordBatch != null)
        {
            final int recordBatchLimit = offset +
                    RecordBatchFW.FIELD_OFFSET_LENGTH + BitUtil.SIZE_OF_INT + recordBatch.length();
            requestedOffset = getRequestedOffsetForPartition.apply(topicName, partition);
            if (recordBatchLimit > recordSetLimit)
            {
                if (recordBatch.lastOffsetDelta() == 0 && recordBatch.length() > messageSizeLimit)
                {
                    System.out.format("[nukleus-kafka] skipping topic: %s partition: %d offset: %d batch-size: %d\n",
                                      topicName, partition, recordBatch.firstOffset(), recordBatch.length());
                    nextFetchAt = recordBatch.firstOffset() + 1;
                    topicDispatcher.flush(partition, requestedOffset, nextFetchAt);
                }
                decoderState = this::decodeSkipRecordSet;
            }
            else
            {
                firstOffset = recordBatch.firstOffset();
                lastOffsetDelta = recordBatch.lastOffsetDelta();
                firstTimestamp = recordBatch.firstTimestamp();
                recordCount = recordBatch.recordCount();
                nextFetchAt = firstOffset;
                decoderState = this::decodeRecord;
                newOffset = recordBatch.limit();
            }
        }
        return newOffset;
    }

    private int decodeSkipRecordSet(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (limit >= recordSetLimit)
        {
            decoderState = this::decodePartitionResponse;
            newOffset =  recordSetLimit;
        }
        else
        {
            newOffset =  limit;
        }
        return newOffset;
    }

    private int decodeRecordLength(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (recordCount > 0)
        {
            Varint32FW recordLength = varint32RO.tryWrap(buffer, offset, limit);
            if (recordLength != null)
            {
                if (offset + recordLength.value()  > recordSetLimit)
                {
                    // Truncated record at end of batch, make sure we attempt to re-fetch it
                    nextFetchAt = firstOffset + lastOffsetDelta;
                    if (recordLength.value() > messageSizeLimit)
                    {
                        // too large to handle, skip it
                        nextFetchAt++;
                    }
                    topicDispatcher.flush(partition, requestedOffset, nextFetchAt);
                    decoderState = this::decodeSkipRecordSet;
                }
                else
                {
                    recordLimit = offset + recordLength.limit();
                    decoderState = this::decodeRecord;
                }
            }
        }
        else
        {
            // If there are deleted records, the last offset reported on record batch may exceed the offset of the
            // last record in the batch. We must use this to make sure we continue to advance.
            nextFetchAt = firstOffset + lastOffsetDelta + 1;
            topicDispatcher.flush(partition, requestedOffset, nextFetchAt);
            decoderState = this::decodePartitionResponse;
        }
        return newOffset;
    }

    private int decodeRecord(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        RecordFW record = recordRO.tryWrap(buffer, offset, limit);
        if (record != null && limit >= recordLimit)
        {
            recordCount--;
            final long currentFetchAt = firstOffset + record.offsetDelta();
            if (currentFetchAt >= requestedOffset)
            {
                // The only guarantee is the response will encompass the requested offset.
                nextFetchAt = currentFetchAt + 1;

                int headersOffset = record.limit();
                final int headerCount = record.headerCount();
                int headersLimit = headersOffset;
                for (int i = 0; i < headerCount; i++)
                {
                    final HeaderFW header = headerRO.wrap(buffer, headersLimit, limit);
                    headersLimit = header.limit();
                }
                headers.wrap(buffer, headersOffset, headersLimit);

                DirectBuffer key = null;
                final OctetsFW messageKey = record.key();
                if (messageKey != null)
                {
                    keyBuffer.wrap(messageKey.buffer(), messageKey.offset(), messageKey.sizeof());
                    key = keyBuffer;
                }

                final long timestamp = firstTimestamp + record.timestampDelta();

                DirectBuffer value = null;
                final OctetsFW messageValue = record.value();
                if (messageValue != null)
                {
                    valueBuffer.wrap(messageValue.buffer(), messageValue.offset(), messageValue.sizeof());
                    value = valueBuffer;
                }
                topicDispatcher.dispatch(partition, requestedOffset, nextFetchAt,
                             key, headers::supplyHeader, timestamp, traceId, value);
            }
            newOffset = recordLimit;
            decoderState = this::decodeRecordLength;
        }
        return newOffset;
    }

    @FunctionalInterface
    interface DecoderState
    {
        int decode(
            DirectBuffer buffer,
            int offset,
            int limit,
            long traceId);
    }

    private static final class Headers
    {
        private int offset;
        private int limit;
        private DirectBuffer buffer;

        private final HeaderFW headerRO = new HeaderFW();
        private final UnsafeBuffer header = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private final UnsafeBuffer value1 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private final UnsafeBuffer value2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

        void wrap(DirectBuffer buffer,
                  int offset,
                  int limit)
        {
            this.buffer = buffer;
            this.offset = offset;
            this.limit = limit;
        }

        DirectBuffer supplyHeader(DirectBuffer headerName)
        {
            DirectBuffer result = null;
            if (limit > offset)
            {
                for (int offset = this.offset; offset < limit; offset = headerRO.limit())
                {
                    headerRO.wrap(buffer, offset, limit);
                    if (matches(headerRO.key(), headerName))
                    {
                        OctetsFW value = headerRO.value();
                        header.wrap(value.buffer(), value.offset(), value.sizeof());
                        result = header;
                        break;
                    }
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
    }
}