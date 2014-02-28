package com.b6w7.eece411.P02;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Immutable class which contains the data for one iteration of test with {@link Node}
 * @author Scott Hazlett
 * @author Ishan Sahay
 */
public class TestData {
	final ByteBuffer buffer = ByteBuffer.allocate(
			NodeCommands.LEN_CMD_BYTES
			+NodeCommands.LEN_KEY_BYTES
			+NodeCommands.LEN_VALUE_BYTES);
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

	/**
	 * Constructor that pads key or value with null bytes if under the specified lengths in {@link NodeCommands}
	 * @param cmd command
	 * @param key ByteBuffer with position set to length of key, between [1-32]
	 * @param value ByteBuffer with position set to length of value, between [1-1024]
	 * @param replyCode expected reply (error) code
	 * @param replyValue expected reply value
	 */
	public TestData( byte cmd, ByteBuffer key, ByteBuffer value, byte replyCode, ByteBuffer replyValue ) {

		// check arguments for correctness
		if (null == key) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (key.equals(NodeCommands.CMD_PUT) && null == value) {
			throw new IllegalArgumentException("value cannot be null for PUT operation");
		}
		if (key.position() > NodeCommands.LEN_KEY_BYTES 
				|| key.position() == 0 
				|| (null != value && value.position() > NodeCommands.LEN_KEY_BYTES)) {
			throw new IllegalArgumentException("key must be between [1-32] bytes and value must be between [1-1024] bytes");
		}

		// Save parameters, and 
		// Place {Cmd, Key, Value} into ByteBuffer 
		// to be ready to be sent down a pipe.  
		// If Key or Value is shorter than 32B and 1024B, respectively,
		// pad with null bytes.
		this.cmd = cmd;
		this.key = key;
		for (int i = 0; i < NodeCommands.LEN_KEY_BYTES - key.position(); i++ )
			key.put((byte)0);	
		this.value = value;
		for (int i = 0; i < NodeCommands.LEN_VALUE_BYTES - value.position(); i++ )
			value.put((byte)0);	
		this.replyCode = replyCode;
		this.replyValue = replyValue;
		
		buffer.put(cmd);
		buffer.put(key);
		buffer.put(value);

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
		s.append("] [key=>");
		
		byte[] byteData = key.array();
		for (int i=0; i<byteData.length; i++) {
			s.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}
		
		s.append("] [value=>");
		try {
			s.append(new String(value.array(), StandardCharsets.UTF_8.displayName()));
		} catch (UnsupportedEncodingException e) {
			s.append(new String(value.array()));
		}
		
		s.append("] [expected reply=>" + replyCode + "]");

		if (NodeCommands.CMD_GET == cmd)
			s.append(" [expected reply value=>"+replyValue+"]");

		return s.toString();
	}
}
