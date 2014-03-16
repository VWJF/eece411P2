package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Arrays;

// ByteArrayWrapper code obtained from 
// http://stackoverflow.com/questions/1058149/using-a-byte-array-as-hashmap-key-java
public final class ByteArrayWrapper implements Comparable<ByteArrayWrapper>
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

    public byte[] getData() {
		return data;
	}

	@Override
	public int compareTo(ByteArrayWrapper arg0) {
		// TODO Auto-generated method stub
//		System.out.println("arg0: "+arg0.getData().length);
//		System.out.println("arg0: "+NodeCommands.byteArrayAsString(arg0.getData()));
//		System.out.println("this: "+data.length);
//		System.out.println("this: "+NodeCommands.byteArrayAsString(data));
//
//		if (Arrays.equals(arg0.getData(), data)){
//			System.out.println("Return 0");
//			return 0;
//		}
//		System.out.println("*Return "+(arg0.getData().length - this.data.length));
//
//		return arg0.getData().length - this.data.length;
		return ByteBuffer.wrap(data).compareTo(ByteBuffer.wrap(arg0.getData()));

	}
	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();

		//Show as Bytes
		s.append("[key=>");
		if (null != data) {
			for (int i=0; i<data.length; i++)
				s.append(Integer.toString((data[i] & 0xff) + 0x100, 16).substring(1));
		} else {
			s.append("null");
		}
		s.append("]");
		
		//Show as String
//		s.append("[key-string=>");
//		if (null != data) {
//			s.append(new String(data));
//		} else {
//			s.append("null");
//		}
//		s.append("]");


		return s.toString();
	}

}