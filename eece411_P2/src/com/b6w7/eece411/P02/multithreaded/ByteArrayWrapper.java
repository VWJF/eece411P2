package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Arrays;

// ByteArrayWrapper code obtained from 
// http://stackoverflow.com/questions/1058149/using-a-byte-array-as-hashmap-key-java
public final class ByteArrayWrapper implements Comparable<ByteArrayWrapper>
{
    public final byte[] key;
    public final ByteBuffer keyBuffer;
    public final byte[] rawKey;


    public ByteArrayWrapper( byte[] raw_key){
    	if (raw_key == null)
    	{
    		throw new NullPointerException();
    	}
    	this.key = raw_key;
    	this.keyBuffer = ByteBuffer.wrap(raw_key);
    	this.rawKey = raw_key;
    }
	
	public ByteArrayWrapper(byte[] hash_of_key, byte[] raw_key)
    {
        if (hash_of_key == null)
        {
            throw new NullPointerException();
        }
        this.key = hash_of_key;
        this.keyBuffer = ByteBuffer.wrap(hash_of_key);
        this.rawKey = raw_key;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayWrapper))
        {
            return false;
        }
        return Arrays.equals(key, ((ByteArrayWrapper)other).key);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(key);
    }

//    public byte[] getData() {
//		return key;
//	}
//	public ByteBuffer getKeyBuffer() {
//		return keyBuffer;
//	}

	@Override
	public int compareTo(ByteArrayWrapper arg0) {
		// TODO Auto-generated method stub
		return keyBuffer.compareTo(arg0.keyBuffer);
	}
	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();

		//Show as Bytes
		s.append("[key=>");
		if (null != key) {
			for (int i=0; i<key.length; i++)
				s.append(Integer.toString((key[i] & 0xff) + 0x100, 16).substring(1));
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