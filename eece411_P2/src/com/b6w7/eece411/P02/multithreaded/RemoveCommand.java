package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

public class RemoveCommand extends Command {
	final ByteBuffer buffer;//= ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;

	byte replyCode;
	ByteBuffer replyValue;

	// protocol for Request: remove command <cmd,key>
	// protocol for Response: <cmd>
	public RemoveCommand(byte cmd, ByteBuffer key, Map<String, String> map) {
		// check arguments for correctness
		if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) {
			throw new IllegalArgumentException("key must be 32 bytes for all operations");
		}

		if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
			throw new IllegalArgumentException("key must be 32 bytes for Remove operation");

		buffer = ByteBuffer.allocate(
				NodeCommands.LEN_CMD_BYTES
				+NodeCommands.LEN_KEY_BYTES);
		//value = null;



		// TODO error check these values
		this.map = map;

		// Save parameters, and 
		// Place {Cmd, Key, Value} into ByteBuffer 
		// to be ready to be sent down a pipe.  
		this.cmd = cmd;
		this.key = key;
		//this.value = value;

		buffer.put(cmd);
		key.rewind();
		buffer.put(key);

		/*
				 if (null != value) {

					value.rewind();
					buffer.put(value);
				}
		 */
	}

	@Override
	public void execute() {	
		if( remove() != null ){  
			this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
		}
		else{
			this.replyCode = (byte) Reply.RPY_INEXISTENT.getCode();
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

	/*
	 * removes the (key,value) pair from the data structure. 
	 * returns the value if the key was present in the structure, null otherwise.
	 */
	private String remove(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		
		StringBuilder k = new StringBuilder();
		try {
			k.append(new String(this.key.array(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			k.append(new String(this.key.array()));
		}
		
		String removed = map.remove(k.toString());
		//if(removed == null)
		//	Command.numElements--;

		return removed;
	}
	
	@Override
	public String toString(){

		//String k = new String( key.array() );
		String s = NodeCommands.requestByteArrayToString(buffer.array());
		return Thread.currentThread().getName() + " "+ s.toString();
	}
}
