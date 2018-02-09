package org.reaktivity.nukleus.kafka.internal.util;

public final class FrameFlags
{
    public static final int EMPTY = 0x00;
    public static final int FIN = 0x01;
    public static final int RST = 0x02;

    public static boolean isEmpty(int flags)
    {
        return (EMPTY & flags) == EMPTY;
    }

    public static boolean isFin(int flags)
    {
        return (FIN & flags) == FIN;
    }

    public static boolean isReset(int flags)
    {
        return (RST & flags) == RST;
    }

    private FrameFlags()
    {
        // Utility class, static methods only
    }

}