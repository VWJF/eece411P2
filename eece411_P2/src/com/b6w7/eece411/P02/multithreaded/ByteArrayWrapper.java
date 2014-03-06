package com.b6w7.eece411.P02.multithreaded;

import java.util.Arrays;

// ByteArrayWrapper code obtained from 
// http://stackoverflow.com/questions/1058149/using-a-byte-array-as-hashmap-key-java
public final class ByteArrayWrapper
{
    private final byte[] data;

    public ByteArrayWrapper(byte[] data)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.data = data;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayWrapper))
        {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper)other).data);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }
}