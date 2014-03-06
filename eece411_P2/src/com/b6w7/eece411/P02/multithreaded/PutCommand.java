package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

public class PutCommand extends Command {
	final byte cmd;
	final byte[] key;
	final byte[] value;

	byte replyCode;

	// protocol for Request: put command <cmd,key,value>
	// protocol for Response: <cmd>
	public PutCommand(byte cmd, byte[] key, byte[] value, Map<byte[], byte[]> map) {
		// check arguments for correctness
		if (null == key || key.length != NodeCommands.LEN_KEY_BYTES) {
			throw new IllegalArgumentException("key must be 32 bytes for all operations");
		}

		if (null == value || value.length != NodeCommands.LEN_VALUE_BYTES) 
			throw new IllegalArgumentException("value must be 1024 bytes for PUT operation");

		// TODO error check these values
		this.map = map;

		// Save parameters, and 
		// Place {Cmd, Key, Value} into ByteBuffer 
		// to be ready to be sent down a pipe.  
		this.cmd = cmd;
		this.key = Arrays.copyOf(key, key.length);
		this.value = Arrays.copyOf(value, value.length);;
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
	public byte[] getReply(){

		ByteBuffer response;

		response = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
		response.put(replyCode);
		
		return response.array();
	}

	private boolean put(){
		//		StringBuilder s = new StringBuilder();
		//		StringBuilder k = new StringBuilder();
		//
		//		try {
		//			s.append(new String(this.value.array(), "UTF-8"));
		//		} catch (UnsupportedEncodingException e) {
		//			s.append(new String(this.value.array()));
		//		}
		//
		//		try {
		//			k.append(new String(this.key.array(), "UTF-8"));
		//		} catch (UnsupportedEncodingException e) {
		//			k.append(new String(this.key.array()));
		//		}
		//		
		//
		//		System.out.println("put (key,value): ("+k.toString()+", "+s.toString()+")");
		//		System.out.println("put key bytes: "+NodeCommands.byteArrayAsString(key.array()) );
		//		System.out.println("put value bytes: "+NodeCommands.byteArrayAsString((s.toString().getBytes())) );
		//
				if(map.size() == MAX_MEMORY && map.containsKey(key) == false ){
					//System.out.println("reached MAX MEMORY "+MAX_MEMORY+" with: ("+k.toString()+", "+s.toString()+")");
					//replyCode = NodeCommands.RPY_OUT_OF_SPACE;
					return false;
				} else {

//					byte[] val = map.get( key );
					
					System.out.println("get key bytes: "+NodeCommands.byteArrayAsString(key) );
					System.out.println("key.length: "+key.length);
					
					if(value != null) {
						System.out.println("get value bytes: "+NodeCommands.byteArrayAsString(value) );
						System.out.println("value.length: "+value.length);
					}

					map.put(key, value);
					
					System.out.println("TESTING POST PUT COMMAND");
					byte[] test = map.get(key);
					
					System.out.println("test bytes: "+NodeCommands.byteArrayAsString(test) );
					System.out.println("test.length: "+test.length);
					return true;
				}
		//		System.out.println("key.length: "+k.length() +
		//							" this.key.array.length: "+this.key.array().length);
		//		System.out.println("value.length: "+s.length() +
		//						" this.value.array.length: "+this.value.array().length);
		//		
		//		if (k.length() != 32 || s.length() != 1024){
		//			System.out.println("****");
		//		}
		//		
		//		map.put(k.toString(),s.toString() );
		//		//	Command.numElements++;
		//
		//		return true;
		//		//replyCode = NodeCommands.RPY_SUCCESS;
		//		//reply.replyCommand(this);
	}

	@Override
	public String toString(){

		//String k = new String( key.array() );
//		String s = NodeCommands.requestByteArrayToString(buffer.array());
		return Thread.currentThread().getName();
	}

}
