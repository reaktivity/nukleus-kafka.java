package org.reaktivity.nukleus.kafka.internal.cache;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;

public interface MessageCache
{

    int NO_MESSAGE = -1;

    MessageFW get(
        int messageHandle,
        MessageFW message);

    int put(
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value);

    int release(
        int messageHandle);

    int replace(
        int messageHandle,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value);

}