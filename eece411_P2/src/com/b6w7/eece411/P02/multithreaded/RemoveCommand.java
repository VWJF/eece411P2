package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

public class RemoveCommand extends Command {
	final byte cmd;
	final byte[] key;

	byte replyCode;
	byte[] replyValue;

	// protocol for Request: remove command <cmd,key>
	// protocol for Response: <cmd>
	public RemoveCommand(byte cmd, byte[] key, Map<byte[], byte[]> map) {
		// check arguments for correctness
		if (null == key || key.length != NodeCommands.LEN_KEY_BYTES) {
			throw new IllegalArgumentException("key must be 32 bytes for all operations");
		}

		if (null == key || key.length != NodeCommands.LEN_KEY_BYTES) 
			throw new IllegalArgumentException("key must be 32 bytes for Remove operation");

		// TODO error check these values
		this.map = map;

		// Save parameters, and 
		// Place {Cmd, Key, Value} into ByteBuffer 
		// to be ready to be sent down a pipe.  
		this.cmd = cmd;
		this.key = Arrays.copyOf(key, key.length);
	}

	@Override
	public void execute() {	
		replyValue = remove();
		
		if( replyValue != null ){  
			this.replyCode = Reply.RPY_SUCCESS.getCode(); 
		}
		else{
			this.replyCode = Reply.RPY_INEXISTENT.getCode();
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
		
		if(replyValue != null){
			response = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES + NodeCommands.LEN_VALUE_BYTES);
			response.put(replyCode);
			response.put(replyValue);
		} else {
			response = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
			response.put(replyCode);
		}
		
		return response.array();
	}

	/*
	 * removes the (key,value) pair from the data structure. 
	 * returns the value if the key was present in the structure, null otherwise.
	 */
	private byte[] remove(){
		return map.remove(key);
		
//		StringBuilder k = new StringBuilder();
//		try {
//			k.append(new String(this.key.array(), "UTF-8"));
//		} catch (UnsupportedEncodingException e) {
//			k.append(new String(this.key.array()));
//		}
//		
//		String removed = map.remove(k.toString());
//		//if(removed == null)
//		//	Command.numElements--;
//
//		return removed;
	}
	
	@Override
	public String toString(){

		//String k = new String( key.array() );
//		String s = NodeCommands.requestByteArrayToString(buffer.array());
		return Thread.currentThread().getName();
	}
}
