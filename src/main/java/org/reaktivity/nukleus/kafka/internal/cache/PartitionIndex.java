package org.reaktivity.nukleus.kafka.internal.cache;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public interface IPartitionIndex
{
    int NO_MESSAGE = -1;
    int TOMBSTONE_MESSAGE = -2;

    public interface Entry
    {
        long offset();

        int message();
    }

    void add(
        long requestOffset,
        long nextFetchOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value);

    Iterator<Entry> entries(
        long requestOffset);

    Entry getEntry(
        long requestOffset,
        OctetsFW key);

}