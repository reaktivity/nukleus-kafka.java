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
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaError.NONE;
import static org.reaktivity.nukleus.kafka.internal.stream.KafkaError.asKafkaError;

import java.util.function.Function;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.function.KafkaErrorConsumer;
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
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;

/**
 * Assumptions about the Kafka fetch response format
 *
 * o Each partition response contains one record set
 * o Record sets may contain 0 or more record batches
 * o The record set length gives the actual number of bytes in the following record batches
 * o The last record batch in a record set may be truncated (if it would exceed the max fetch bytes
 *   or max partition bytes set in the fetch request). In this case its length may or may not exceed
 *   the remaining bytes of record set length.
 * o When a record batch is truncated, it may be truncated at a record boundary (i.e. one or
 *   more records at the end are absent), or in the middle of a record. In the latter case, the record
 *   length field (which is the first field, and is a varint) will always be present, and not truncated.
 */
public class FetchResponseDecoder implements ResponseDecoder
{
    private final ResponseHeaderFW responseRO = new ResponseHeaderFW();
    private final FetchResponseFW fetchResponseRO = new FetchResponseFW();
    private final TopicResponseFW topicResponseRO = new TopicResponseFW();
    private final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    private final TransactionResponseFW transactionResponseRO = new TransactionResponseFW();
    private final RecordSetFW recordSetRO = new RecordSetFW();

    private final Varint32FW varint32RO = new Varint32FW();

    private final RecordBatchFW recordBatchRO = new RecordBatchFW();
    private final RecordFW recordRO = new RecordFW();
    private final HeaderFW headerRO = new HeaderFW();

    private final HeadersFW headers = new HeadersFW();
    private final DirectBuffer keyBuffer = new UnsafeBuffer(BufferUtil.EMPTY_BYTE_ARRAY);
    private final DirectBuffer valueBuffer = new UnsafeBuffer(BufferUtil.EMPTY_BYTE_ARRAY);

    private final Function<String, DecoderMessageDispatcher> getDispatcher;
    private final StringIntToLongFunction getRequestedOffsetForPartition;
    private final KafkaErrorConsumer errorHandler;
    private final int maxRecordBatchSize;
    private final MutableDirectBuffer buffer;

    private DecoderState decoderState;
    private int responseBytesRemaining;
    private int slotOffset;
    private int slotLimit;

    private int topicCount;
    private String topicName;
    private DecoderMessageDispatcher messageDispatcher;
    private int partitionCount;
    private short errorCode;
    private int recordSetBytesRemaining;
    private int recordBatchBytesRemaining;
    private long highWatermark;
    private int partition;
    private int abortedTransactionCount;
    private long requestedOffset;
    private long nextFetchAt;
    private int recordCount;
    private long firstOffset;
    private int lastOffsetDelta;
    private long firstTimestamp;

    private final SkipBytesDecoderState skipBytesDecoderState = new SkipBytesDecoderState();

    private final BufferBytesDecoderState bufferBytesDecoderState = new BufferBytesDecoderState();

    FetchResponseDecoder(
        Function<String, DecoderMessageDispatcher> getDispatcher,
        StringIntToLongFunction getRequestedOffsetForPartition,
        KafkaErrorConsumer errorHandler,
        MutableDirectBuffer decodingBuffer)
    {
        this.getDispatcher = getDispatcher;
        this.getRequestedOffsetForPartition = getRequestedOffsetForPartition;
        this.errorHandler = errorHandler;
        this.buffer = requireNonNull(decodingBuffer);
        this.maxRecordBatchSize = buffer.capacity();
        this.decoderState = this::decodeResponseHeader;
    }

    @Override
    public int decode(
        OctetsFW payload,
        long traceId)
    {
        int remaining = decodePayload(payload.buffer(), payload.offset(), payload.limit(), traceId);

        // Process any bytes which could not be fitted into the decoding buffer (which is sized to the
        // maximum size of a partition response, so this concerns bytes following a partition response)
        if (remaining > 0)
        {
            int newOffset = payload.limit() - remaining;
            remaining = decodePayload(payload.buffer(), newOffset, payload.limit(), traceId);
        }

        return responseBytesRemaining == 0 ? remaining : -responseBytesRemaining;
    }

    private int decodePayload(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        int unconsumedBytes = 0;
        if (slotLimit > 0)
        {
            unconsumedBytes = appendToSlot(buffer, offset, limit);
            buffer = this.buffer;
            offset = slotOffset;
            limit = slotLimit;
        }
        int newOffset = decode(buffer, offset, limit, traceId);
        responseBytesRemaining -= newOffset - offset;
        if (responseBytesRemaining == 0)
        {
            assert newOffset == limit : "no pipelined requests";
            reinitialize();
        }
        else if (newOffset == limit)
        {
            slotOffset = slotLimit = 0;
        }
        else if (slotLimit == 0)
        {
            slotOffset = 0;
            slotLimit = 0;
            unconsumedBytes = appendToSlot(buffer, newOffset, limit);
        }
        else
        {
             slotOffset = newOffset;
        }
        return unconsumedBytes;
    }

    @Override
    public void reinitialize()
    {
        slotOffset = 0;
        slotLimit = 0;
        responseBytesRemaining = 0;
        topicCount = 0;
        partitionCount = 0;
        decoderState = this::decodeResponseHeader;
    }

    private int appendToSlot(DirectBuffer source, int offset, int limit)
    {
        final int remaining = buffer.capacity() - (slotLimit - slotOffset);
        final int bytesToCopy = limit -  offset;
        final int bytesCopied = Math.min(bytesToCopy, remaining);
        if (bytesCopied + slotLimit > buffer.capacity())
        {
            alignSlotData(buffer);
        }
        buffer.putBytes(slotLimit, source, offset, bytesCopied);
        slotLimit += bytesCopied;
        return bytesToCopy - bytesCopied;
    }

    private void alignSlotData(MutableDirectBuffer slot)
    {
        int dataLength = slotLimit - slotOffset;
        slot.putBytes(0, slot, slotOffset, dataLength);
        slotOffset = 0;
        slotLimit = dataLength;
    }

    private int decode(
        DirectBuffer buffer,
        int offset,
        int limit,
        long traceId)
    {
        boolean workWasDone = true;
        while (workWasDone)
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
            int totalResponseBytes = response.size() + response.sizeof();
            responseBytesRemaining = totalResponseBytes;
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
            decoderState = this::decodeTopicResponse;
        }
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
                messageDispatcher = getDispatcher.apply(topicName);
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
                errorCode = response.errorCode();
                highWatermark = response.highWatermark();
                abortedTransactionCount = response.abortedTransactionCount();
                requestedOffset = getRequestedOffsetForPartition.apply(topicName, partition);
                nextFetchAt = requestedOffset;
                decoderState = abortedTransactionCount > 0 ? this::decodeTransactionResponse : this::decodeRecordSet;
                if (errorCode != NONE.errorCode)
                {
                    errorHandler.accept(topicName, partition, asKafkaError(errorCode));
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
            recordSetBytesRemaining = response.recordSetSize();
            assert recordSetBytesRemaining >= 0 : "protocol violation: negative recordSetSize";
            assert recordSetBytesRemaining + response.sizeof() <= responseBytesRemaining :
                "protocol violation, record set goes beyond end of response";
            newOffset = response.limit();
            if (recordSetBytesRemaining == 0)
            {
                decoderState = this::decodePartitionResponse;
            }
            else if (recordSetBytesRemaining > maxRecordBatchSize)
            {
                System.out.format(
                        "[nukleus-kafka] skipping topic: %s partition: %d offset: %d because record set size %d " +
                        "exceeds configured nukleus.kafka.fetch.partition.max.bytes %d\n",
                        topicName, partition, requestedOffset, recordSetBytesRemaining, maxRecordBatchSize);
                messageDispatcher.flush(partition, requestedOffset, requestedOffset + 1);
                skipBytesDecoderState.bytesToSkip = recordSetBytesRemaining;
                skipBytesDecoderState.nextState = this::decodePartitionResponse;
                decoderState = skipBytesDecoderState;
            }
            else
            {
                // Buffer the record set so we can compact records, and dispatch them without
                // allocating buffers for each message key, headers, and value
                bufferBytesDecoderState.bytesToAwait = recordSetBytesRemaining;
                bufferBytesDecoderState.nextState = this::decodeRecordBatch;
                decoderState = bufferBytesDecoderState;
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
        int boundedLimit = Math.min(offset + recordSetBytesRemaining,  limit);
        RecordBatchFW recordBatch = recordBatchRO.tryWrap(buffer, offset, boundedLimit);
        if (recordBatch == null)
        {
            // Truncated record batch at end of record set
            if (nextFetchAt > requestedOffset)
            {
                messageDispatcher.flush(partition, requestedOffset, nextFetchAt);
            }
            skipBytesDecoderState.bytesToSkip = recordSetBytesRemaining;
            skipBytesDecoderState.nextState = this::decodePartitionResponse;
            decoderState = skipBytesDecoderState;
        }
        else
        {
            final int recordBatchActualSize =
                    RecordBatchFW.FIELD_OFFSET_LENGTH + BitUtil.SIZE_OF_INT + recordBatch.length();
            if (recordBatchActualSize > recordSetBytesRemaining
                && recordBatch.lastOffsetDelta() == 0 && recordBatchActualSize > maxRecordBatchSize)
                // record batch was truncated in response due to max fetch bytes or max partition bytes limit set on request,
                // contains only one (necessarily incomplete) message
            {
                    System.out.format(
                        "[nukleus-kafka] skipping large message at topic: %s partition: %d offset: %d, " +
                            "message size %d bytes exceeds configured nukleus.kafka.fetch.partition.max.bytes %d\n",
                        topicName, partition, recordBatch.firstOffset(), recordBatch.length(), maxRecordBatchSize);
                    nextFetchAt = recordBatch.firstOffset() + 1;
                    messageDispatcher.flush(partition, requestedOffset, nextFetchAt);
                    skipBytesDecoderState.bytesToSkip = recordSetBytesRemaining;
                    skipBytesDecoderState.nextState = this::decodePartitionResponse;
                    decoderState = skipBytesDecoderState;
            }
            else
            {
                firstOffset = recordBatch.firstOffset();
                lastOffsetDelta = recordBatch.lastOffsetDelta();
                firstTimestamp = recordBatch.firstTimestamp();
                recordCount = recordBatch.recordCount();
                nextFetchAt = firstOffset;
                recordSetBytesRemaining -= recordBatch.sizeof();
                assert recordSetBytesRemaining >= 0;
                recordBatchBytesRemaining = recordBatchActualSize - recordBatch.sizeof();
                assert recordBatchBytesRemaining >= 0;
                decoderState = this::decodeRecordLength;
                newOffset = recordBatch.limit();
            }
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
        if (recordSetBytesRemaining == 0)
        {
            if (recordBatchBytesRemaining == 0)
            {
                // If there are deleted records, the last offset reported on the record batch may exceed the
                // offset of the last record in the batch. So we must use this to make sure we continue to advance.
                nextFetchAt = firstOffset + lastOffsetDelta + 1;
            }
            messageDispatcher.flush(partition, requestedOffset, nextFetchAt);
            decoderState = this::decodePartitionResponse;
        }
        else if (recordBatchBytesRemaining == 0)
        {
            decoderState = this::decodeRecordBatch;
        }
        else
        {
            assert recordCount > 0 : "protocol violation: excess bytes following partition response or incorrect record count";
            Varint32FW recordLength = varint32RO.tryWrap(buffer, offset, limit);
            int recordSize = -1;
            if (recordLength != null)
            {
                recordSize = recordLength.sizeof() + recordLength.value();
            }
            else
            {
                // record length truncated, record size must be GT recordSetBytesRemaining
                assert limit - offset == recordSetBytesRemaining;
                recordSize = recordSetBytesRemaining + 1;
            }
            if (recordSize > recordSetBytesRemaining)
            {
                // Truncated record at end of batch, make sure we attempt to re-fetch it. Experience shows
                // the record batch's lastOffsetDelta is that of the last record that would have been in
                // in the batch had it not been truncated, so we must re-fetch this record using the
                // the offset of the previous record plus 1 (if there was a previous record) else the
                // first offset of the record batch.
                messageDispatcher.flush(partition, requestedOffset, nextFetchAt);
                skipBytesDecoderState.bytesToSkip = recordSetBytesRemaining;
                skipBytesDecoderState.nextState = this::decodePartitionResponse;
                decoderState = skipBytesDecoderState;
            }
            else
            {
                assert recordSize <= recordBatchBytesRemaining :
                    "protocol violation: truncated record batch not last in record set";
                recordBatchBytesRemaining -= recordSize;
                recordSetBytesRemaining -= recordSize;
                decoderState = this::decodeRecord;
                if (recordSetBytesRemaining < 0)
                {
                    System.out.println(String.format(
                            "W2: recordCount=%d, recordSize=%d, recordSetBytesRemaining=%d, recordBatchBytesRemaining=%d",
                            recordCount, recordSize, recordSetBytesRemaining, recordBatchBytesRemaining));
                }
            }
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
        if (record != null)
        {
            recordCount--;
            final long currentFetchAt = firstOffset + record.offsetDelta();
            int headersOffset = record.limit();
            int headersLimit = headersOffset;
            final int headerCount = record.headerCount();
            for (int i = 0; i < headerCount; i++)
            {
                final HeaderFW header = headerRO.wrap(buffer, headersLimit, limit);
                headersLimit = header.limit();
            }
            if (currentFetchAt >= requestedOffset)
                // The only guarantee is the response will encompass the requested offset.
            {
                nextFetchAt = currentFetchAt + 1;

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
                headers.wrap(buffer, headersOffset, headersLimit);
                messageDispatcher.dispatch(partition, requestedOffset, currentFetchAt, highWatermark,
                        key, headers, timestamp, traceId, value);
            }
            newOffset = headersLimit;
            decoderState = this::decodeRecordLength;
        }
        return newOffset;
    }

    @FunctionalInterface
    interface DecoderState
    {
        /**
         * @return The new offset. If unchanged from offset, more data is required.
         */
        int decode(
            DirectBuffer buffer,
            int offset,
            int limit,
            long traceId);
    }

    final class BufferBytesDecoderState implements DecoderState
    {
        int bytesToAwait;
        DecoderState nextState;

        @Override
        public int decode(
            DirectBuffer buffer,
            int offset,
            int limit,
            long traceId)
        {
            int availableBytes = limit - offset;
            if (availableBytes >= bytesToAwait)
            {
                decoderState = nextState;
            }
            return offset;
        }
    }

    final class SkipBytesDecoderState implements DecoderState
    {
        int bytesToSkip;
        DecoderState nextState;

        @Override
        public int decode(
            DirectBuffer buffer,
            int offset,
            int limit,
            long traceId)
        {
            int bytesSkipped = Math.min(limit - offset, bytesToSkip);
            int newOffset = offset + bytesSkipped;
            bytesToSkip -= bytesSkipped;
            if (bytesToSkip == 0)
            {
                decoderState = nextState;
            }
            return newOffset;
        }
    }
}
