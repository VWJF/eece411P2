package com.b6w7.eece411.P02.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.b6w7.eece411.P02.Node;
import com.b6w7.eece411.P02.NodeCommands;

/**
 * Immutable class which contains the data for one iteration of test with {@link Node}
 * @author Scott Hazlett
 * @author Ishan Sahay
 */
public class TestData {
	final ByteBuffer buffer;
	final byte cmd;
	final ByteBuffer key;
	final ByteBuffer value;
	final byte replyCode;
	final ByteBuffer replyValue;
	final int index;

	private static Integer _index = new Integer(0);

	@SuppressWarnings("unused")
	private TestData() {
		throw new UnsupportedOperationException("private constructor");
	}

	/**
	 * Constructor for a test vector used in {@link TestNode}
	 * @param cmd command
	 * @param key ByteBuffer with limit of 32
	 * @param value ByteBuffer of sent value with limit of 1024 if PUT command, otherwise ignored
	 * @param replyCode expected reply code
	 * @param replyValue ByteBuffer of expected reply with limit of 1024 if GET command, otherwise ignored
	 */
	public TestData( byte cmd, ByteBuffer key, ByteBuffer value, byte replyCode, ByteBuffer replyValue ) {

		// check arguments for correctness
		if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) {
			throw new IllegalArgumentException("key must be 32 bytes for all operations");
		}


		if (NodeCommands.CMD_PUT == cmd) {
			if (null == value || value.limit() != NodeCommands.LEN_VALUE_BYTES) 
				throw new IllegalArgumentException("value must be 1024 bytes for PUT operation");

			buffer = ByteBuffer.allocate(
					NodeCommands.LEN_CMD_BYTES
					+NodeCommands.LEN_KEY_BYTES
					+NodeCommands.LEN_VALUE_BYTES);

		} else if (NodeCommands.CMD_GET == cmd) {
			if (null == replyValue || replyValue.limit() != NodeCommands.LEN_VALUE_BYTES) 
				throw new IllegalArgumentException("replyValue must be 1024 bytes for GET operation");

			buffer = ByteBuffer.allocate(
					NodeCommands.LEN_CMD_BYTES
					+NodeCommands.LEN_KEY_BYTES);

		} else if (NodeCommands.CMD_REMOVE == cmd) {
			buffer = ByteBuffer.allocate(
					NodeCommands.LEN_CMD_BYTES
					+NodeCommands.LEN_KEY_BYTES);

		} else {
			buffer = ByteBuffer.allocate(
					NodeCommands.LEN_CMD_BYTES
					+NodeCommands.LEN_KEY_BYTES
					+NodeCommands.LEN_VALUE_BYTES);
			//throw new IllegalArgumentException("Unknown command");
		}



		// Save parameters, and 
		// Place {Cmd, Key, Value} into ByteBuffer 
		// to be ready to be sent down a pipe.  
		this.cmd = cmd;
		this.key = key;
		this.value = value;
		this.replyCode = replyCode;
		this.replyValue = replyValue;

		buffer.put(cmd);
		key.rewind();
		buffer.put(key);

		if (null != value) {
			value.rewind();
			buffer.put(value);
		}

		synchronized (_index) {
			this.index = _index;
			_index ++;
		}
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();

		s.append("[test index=>"+index+"] [command=>");
		s.append(NodeCommands.Request.values()[cmd].toString());
		s.append("] [key=>");

		byte[] byteData = key.array();
		for (int i=0; i<byteData.length; i++) {
			s.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}

		if (null != value) {
			s.append("] [value=>");
			try {
				// s.append(new String(value.array(), StandardCharsets.UTF_8.displayName()));
				s.append(new String(value.array(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				s.append(new String(value.array()));
			}
		}

		s.append("] [expected reply=>" + replyCode + "]");

		if (NodeCommands.CMD_GET == cmd)
			s.append(" [expected reply value=>"+NodeCommands.requestByteArrayToString(replyValue.array())+"]");

		return s.toString();
	}
}
