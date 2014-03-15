package com.b6w7.eece411.P02.nio;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SocketChannelBuffers {

	private final Object buffersSem = new Object();
	private final int BUFFER_CAPACITY;
	private int index = 0;
	private Map<Integer, ByteBuffer> buffers;

	/**
	 * Keeps many {@link ByteBuffer}s useful for a concurrent non-bocking IO service.
	 * Each {@link ByteBuffer} is represented by a unique Integer key.
	 * This key can be used to retrieve the buffer.  Every {@link ByteBuffer} is page-aligned
	 * with Java 1.6.  Every {@link ByteBuffer} may or may not be page-aligned with Java 1.7 onwards.
	 * @param keyStartingCapacity the expected number of buffers to be created
	 * @param bufferSize the size in bytes of each buffer
	 */
	public SocketChannelBuffers(int keyStartingCapacity, int bufferSize) {
		buffers = new HashMap<Integer, ByteBuffer>((int)(keyStartingCapacity * 1.2));
		BUFFER_CAPACITY = bufferSize;
	}

	/**
	 * Allocates a ByteBuffer and returns a unique Integer representing its key
	 * which can be used to retrieve the buffer via {@code get(Integer key)}
	 * @return Integer key of the ByteBuffer 
	 */
	public Integer allocate() {
		Integer ret;
		ByteBuffer newBuffer = ByteBuffer.allocateDirect(BUFFER_CAPACITY);

		synchronized (buffersSem) {
			do {
				index ++;
			} while (buffers.containsKey(index));

			ret = Integer.valueOf(index);
			buffers.put(ret, newBuffer);
		}
		return ret;
	}

	public ByteBuffer get(Integer key) {
		synchronized (buffersSem) {
			return buffers.get(key);
		}
	}
}
