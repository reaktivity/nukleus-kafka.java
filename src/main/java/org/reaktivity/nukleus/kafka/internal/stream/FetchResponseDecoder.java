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

import java.util.function.BiFunction;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.kafka.internal.function.StringIntToLongFunction;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.ResponseHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.FetchResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordBatchFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.TopicResponseFW;

public class FetchResponseDecoder
{
    final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    final TopicResponseFW topicResponseRO = new TopicResponseFW();
    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();

    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final RecordFW recordRO = new RecordFW();
    private final HeaderFW headerRO = new HeaderFW();

    private final BufferPool bufferPool;
    private final long streamId;

    private DecoderState decoderState;
    private int responseSize = -1;
    private int responseBytesProcessed;
    int slot = NO_SLOT;
    int slotOffset;
    int slotLimit;

    private final BiFunction<String, Integer, MessageDispatcher> getDispatcher;
    private final StringIntToLongFunction getRequestedOffsetForPartition;
    private int topicCount;
    private Object topicName;
    private Object partitionCount;

    FetchResponseDecoder(
        BiFunction<String, Integer, MessageDispatcher> getDispatcher,
        StringIntToLongFunction getRequestedOffsetForPartition,
        BufferPool bufferPool,
        long streamId)
    {
        this.getDispatcher = getDispatcher;
        this.getRequestedOffsetForPartition = getRequestedOffsetForPartition;
        this.bufferPool = requireNonNull(bufferPool);
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
        boolean decoderStateChanged = true;
        while (offset < limit && decoderStateChanged)
        {
            DecoderState previous = decoderState;
            offset = decoderState.decode(buffer, offset, limit, traceId);
            decoderStateChanged = previous != decoderState;
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
        if (offset + BitUtil.SIZE_OF_INT >= limit) // TODO: if responseRO.canWrap
        {
            ResponseHeaderFW response = null;
            response = responseRO.wrap(buffer, offset, limit);
            newOffset = response.limit();
            responseSize = response.size();
        }
        decoderState = this::decodeFetchResponse;
        return newOffset;
    }

    private int decodeFetchResponse(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int newOffset = offset;
        if (offset + 4 * BitUtil.SIZE_OF_INT >= limit) // TODO: if fetchResponseRO.canWrap
        {
            FetchResponseFW response = null;
            response = fetchResponseRO.wrap(buffer, offset, limit);
            newOffset = response.limit();
            topicCount = response.topicCount();
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
        if (offset + BitUtil.SIZE_OF_INT + 50 /*max topic name length*/ >= limit) // TODO: if topicResponseRO.canWrap
        {
            TopicResponseFW response = null;
            response = topicResponseRO.wrap(buffer, offset, limit);
            newOffset = response.limit();
            topicName = response.name().asString();
            partitionCount = response.partitionCount();
        }
        decoderState = this::decodeTopicResponse;
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