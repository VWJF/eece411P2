package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.b6w7.eece411.P02.multithreaded.NodeCommands.Reply;

public class RemoveCommand extends Command {
	final byte[] key;

	byte replyCode = NodeCommands.Reply.CMD_NOT_SET.getCode();
	byte[] replyValue;
	
	// protocol for Request: remove command <cmd,key>
	// protocol for Response: <cmd>
	public RemoveCommand(byte[] key, Map<ByteArrayWrapper, byte[]> map) {
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
		synchronized(execution_completed_sem){
			execution_completed = true;
			execution_completed_sem.notifyAll();
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
//		System.out.println("(key.length, get key bytes): ("+key.length+
//				", "+NodeCommands.byteArrayAsString(key) +")" );
				
		return map.remove(new ByteArrayWrapper(key));

	}

	@Override
	public String toString(){
		StringBuilder s = new StringBuilder();

		s.append("[command=>");
		s.append(NodeCommands.Request.CMD_REMOVE.toString());
		s.append("] [key["+key.length+"]=>");
		for (int i=0; i<LEN_TO_STRING_OF_KEY; i++)
			s.append(Integer.toString((key[i] & 0xff) + 0x100, 16).substring(1));

		s.append("] [replyCode=>");
		s.append(NodeCommands.Reply.values()[replyCode].toString());
		if (replyValue != null) {
			s.append("] [replyValue["+replyValue.length+"]=>");
			for (int i=0; i<LEN_TO_STRING_OF_VAL; i++)
				s.append(Integer.toString((replyValue[i] & 0xff) + 0x100, 16).substring(1));
		}
		s.append("]");

		return s.toString();
	}
}
