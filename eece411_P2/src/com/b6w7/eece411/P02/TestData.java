package com.b6w7.eece411.P02;

import java.nio.ByteBuffer;

/**
 * Immutable class which contains the data for one iteration of test with {@link Node}
 */
public class TestData {
	final ByteBuffer buffer = ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;
	final ByteBuffer value;
	final byte replyCode;
	final ByteBuffer replyValue;
	final int index;

	private static int _index = 0;

	@SuppressWarnings("unused")
	private TestData() {
		throw new UnsupportedOperationException("private constructor");
	}

	public TestData( byte cmd, ByteBuffer key, ByteBuffer value, byte replyCode, ByteBuffer replyValue ) {

		// check arguments for correctness
		if (null == key) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (key.equals(NodeCommands.CMD_PUT) && null == value) {
			throw new IllegalArgumentException("value cannot be null for PUT operation");
		}
		if (key.limit() > 32 || (null != value && value.limit() > 1024)) {
			throw new IllegalArgumentException("key cannot exceed 32 bytes and value cannot exceed 1024 bytes");
		}

		buffer.put(cmd);
		buffer.put(key);
		buffer.put(value);
		this.cmd = cmd;
		this.key = key;
		this.value = value;
		this.replyCode = replyCode;
		this.replyValue = replyValue;

		this.index = _index;
		_index ++;

	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
	
		s.append("[test index=>"+index+"] [command=>");
		switch(cmd) {
		case NodeCommands.CMD_PUT:
			s.append("PUT");
			break;
		case NodeCommands.CMD_GET:
			s.append("GET");
			break;
		case NodeCommands.CMD_REMOVE:
			s.append("REMOVE");
			break;
		default:
			s.append("UNKNOWN");
		}
		s.append("] [key=>"+key+"] [value=>"+value+"] [expected reply=>" + replyCode + "]");
		
		if (NodeCommands.CMD_GET == cmd)
			s.append(" [expected reply value=>"+replyValue+"]");
		
		return s.toString();
	}
}
