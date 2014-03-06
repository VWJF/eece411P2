package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

public class PutCommand extends Command {
	final ByteBuffer buffer;//= ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;

	final ByteBuffer value;

	byte replyCode;
	ByteBuffer replyValue;

	// protocol for Request: put command <cmd,key,value>
	// protocol for Response: <cmd>
	public PutCommand(byte cmd, ByteBuffer key, ByteBuffer value, Map<String, String> map) {
		// check arguments for correctness
		if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) {
			throw new IllegalArgumentException("key must be 32 bytes for all operations");
		}

		if (null == value || value.limit() != NodeCommands.LEN_VALUE_BYTES) 
			throw new IllegalArgumentException("value must be 1024 bytes for PUT operation");

		buffer = ByteBuffer.allocate(
				NodeCommands.LEN_CMD_BYTES
				+NodeCommands.LEN_KEY_BYTES
				+NodeCommands.LEN_VALUE_BYTES);

		// TODO error check these values
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
	}

	@Override
	public void execute() {	
		if( put() ){
			this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
		}
		else{
			this.replyCode = (byte) Reply.RPY_OUT_OF_SPACE.getCode(); 
		}
		synchronized(execution_completed){
			execution_completed = true;
		}
	}
	
	/*
	 * returns the appropriate response to be sent to the client for the requested (command,key,value)
	 */
	@Override
	public ByteBuffer getReply(){
		
		ByteBuffer response = ByteBuffer.allocate( 1 );
		response.put(replyCode);
		if(replyValue != null){
			response = ByteBuffer.allocate( 1 + replyValue.capacity());
			response.put(replyCode);
			replyValue.rewind();
			response.put(replyValue);
		}
		
		return response;
	}

	private boolean put(){

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
			//replyCode = NodeCommands.RPY_OUT_OF_SPACE;
			return false;
		}

		map.put(new String(key.array()), new String(this.value.array()) );
		//	Command.numElements++;

		return true;
		//replyCode = NodeCommands.RPY_SUCCESS;
		//reply.replyCommand(this);
	}

}
