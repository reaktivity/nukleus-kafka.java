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
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaErrors.NONE;

import java.util.function.Function;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
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
    private final int maximumFetchBytesLimit;
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

    FetchResponseDecoder(
        Function<String, MessageDispatcher> getDispatcher,
        StringIntToLongFunction getRequestedOffsetForPartition,
        StringIntShortConsumer errorHandler,
        BufferPool bufferPool,
        int maxFetchBytesLimit,
        long streamId)
    {
        this.getDispatcher = getDispatcher;
        this.getRequestedOffsetForPartition = getRequestedOffsetForPartition;
        this.errorHandler = errorHandler;
        this.bufferPool = requireNonNull(bufferPool);
        this.maximumFetchBytesLimit = maxFetchBytesLimit;
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
                if (recordBatch.lastOffsetDelta() == 0 && recordBatch.length() > maximumFetchBytesLimit)
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

    private int decodeRecord(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (recordCount > 0)
        {
            RecordFW record = recordRO.tryWrap(buffer, offset, limit);
            if (record != null)
            {
                recordCount--;

                // TODO: dispatch the record and adjust nextFetchAt
                topicDispatcher.dispatch();

                newOffset = record.limit();
            }
            else
            {
                Varint32FW recordLength = varint32RO.tryWrap(buffer, offset, recordSetLimit);
                if (recordLength != null && offset + recordLength.value()  > recordSetLimit)
                {
                    // Truncated record at end of batch, make sure we attempt to re-fetch it
                    nextFetchAt = firstOffset + lastOffsetDelta;
                    topicDispatcher.flush(partition, requestedOffset, nextFetchAt);
                    decoderState = this::decodeSkipRecordSet;
                }
            }
        }
        else
        {
            topicDispatcher.flush(partition, requestedOffset, nextFetchAt);
            decoderState = this::decodePartitionResponse;
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
}