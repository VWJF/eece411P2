package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;

public class PutCommand implements Command {
	private static final byte MAX_MEMORY = 3;
	private final Socket clientSock;
	
	final ByteBuffer buffer;//= ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;
	final ByteBuffer value;
	final Map<String, String> map;
	final ReplyCommand reply;
	
	byte replyCode;
	ByteBuffer replyValue;

	
	public PutCommand(Socket client, byte cmd, ByteBuffer key, ByteBuffer value, Map<String, String> map, ReplyCommand reply) {
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
					if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
						throw new IllegalArgumentException("key must be 32 bytes for GET operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES);
					value = null;

				} else if (NodeCommands.CMD_REMOVE == cmd) {
					if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
						throw new IllegalArgumentException("key must be 32 bytes for Remove operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES);
					value = null;
				} else {
					buffer = ByteBuffer.allocate(
						NodeCommands.LEN_CMD_BYTES
						+NodeCommands.LEN_KEY_BYTES
						+NodeCommands.LEN_VALUE_BYTES);
						//throw new IllegalArgumentException("Unknown command");
				}


				// TODO error check these values
				this.reply = reply;
				this.map = map;

				// Save parameters, and 
				// Place {Cmd, Key, Value} into ByteBuffer 
				// to be ready to be sent down a pipe.  
				this.cmd = cmd;
				this.key = key;
				this.value = value;
			
				buffer.put(cmd);
				key.rewind();
				buffer.put(key);

				if (null != value) {
					value.rewind();
					buffer.put(value);
				}

		// check arguments for correctness
		if (client == null) {
			throw new IllegalArgumentException("client socket cannot be null");
		}
		
		this.clientSock = client;
		
	}

	@Override
	public void execute() {
		
		
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		
		StringBuilder s = new StringBuilder();
		/*
		for (int i=0; i<(NodeCommands.LEN_VALUE_BYTES); i++) { 
			s.append(Integer.toString((this.value.array()[i] & 0xff) + 0x100, 16).substring(1));
		}
		*/
		
		try {
			s.append(new String(this.value.array(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			s.append(new String(this.value.array()));
		}
		
		String k = new String(key.array());
		System.out.println("put (key,value): ("+k+", "+s.toString()+")");
		System.out.println("put key bytes: "+NodeCommands.byteArrayAsString(key.array()) );
		System.out.println("put value bytes: "+NodeCommands.byteArrayAsString((s.toString().getBytes())) );

		if(map.size() == MAX_MEMORY && map.containsKey(key) == false ){
			System.out.println("reached MAX MEMORY "+MAX_MEMORY+" with: ("+k+", "+s.toString()+")");
			replyCode = NodeCommands.RPY_OUT_OF_SPACE;
		}
		
		map.put(new String(key.array()), new String(this.value.array()) );
		//	Command.numElements++;
		replyCode = NodeCommands.RPY_SUCCESS;

		reply.replyCommand(this);
	}

}
