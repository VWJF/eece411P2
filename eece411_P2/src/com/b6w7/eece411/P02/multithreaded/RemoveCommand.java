package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

public class RemoveCommand extends Command {
	final byte cmd;
	final byte[] key;

	byte replyCode = NodeCommands.CMD_NOT_SET;
	byte[] replyValue;

	// protocol for Request: remove command <cmd,key>
	// protocol for Response: <cmd>
	public RemoveCommand(byte cmd, byte[] key, Map<ByteArrayWrapper, byte[]> map) {
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
		System.out.println("(key.length, get key bytes): ("+key.length+
				", "+NodeCommands.byteArrayAsString(key) +")" );

		return map.remove(new ByteArrayWrapper(key));

	}

	@Override
	public String toString(){
		StringBuilder s = new StringBuilder();

		s.append("[command=>");
		s.append(NodeCommands.Request.values()[cmd].toString());
		s.append("] [key["+key.length+"]=>");
		for (int i=0; i<key.length; i++)
			s.append(Integer.toString((key[i] & 0xff) + 0x100, 16).substring(1));

		s.append("] [replyCode=>");
		s.append(NodeCommands.Reply.values()[replyCode].toString());
		if (replyValue != null) {
			s.append("] [replyValue["+replyValue.length+"]=>");
			for (int i=0; i<replyValue.length; i++)
				s.append(Integer.toString((replyValue[i] & 0xff) + 0x100, 16).substring(1));
		}
		s.append("]");

		return s.toString();
	}
}
