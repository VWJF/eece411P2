package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.b6w7.eece411.P02.multithreaded.NodeCommands.Reply;

public class PutCommand extends Command {
	final byte[] key;
	final byte[] value;

	byte replyCode = NodeCommands.Reply.RPY_NOT_SET.getCode();

	// protocol for Request: put command <cmd,key,value>
	// protocol for Response: <cmd>
	public PutCommand(byte[] key, byte[] value, Map<ByteArrayWrapper, byte[]> map) {
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
		this.key = Arrays.copyOf(key, key.length);
		this.value = Arrays.copyOf(value, value.length);
	}

	@Override
	public void execute() {	
		if( put() ){
			this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
		}
		else{
			this.replyCode = (byte) Reply.RPY_OUT_OF_SPACE.getCode(); 
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

		response = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
		response.put(replyCode);

		return response.array();
	}
	private boolean put(){

		if(map.size() == MAX_MEMORY && map.containsKey(key) == false ){
			//System.out.println("reached MAX MEMORY "+MAX_MEMORY+" with: ("+k.toString()+", "+s.toString()+")");
			//replyCode = NodeCommands.RPY_OUT_OF_SPACE;
			return false;
			
		} else {
			byte[] result = map.put(new ByteArrayWrapper(key), value);

			if(result != null) {
				// Overwriting -- we take note
				System.out.println("*** PutCommand() Replacing Key " + this.toString());
			}

			return true;
		}
	}

	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();

		s.append("[command=>");
		s.append(NodeCommands.Request.CMD_PUT.toString());
		s.append("] [key=>");
		for (int i=0; i<LEN_TO_STRING_OF_KEY; i++)
			s.append(Integer.toString((key[i] & 0xff) + 0x100, 16).substring(1));

		s.append("] [value["+value.length+"]=>");
		for (int i=0; i<LEN_TO_STRING_OF_VAL; i++)
			s.append(Integer.toString((value[i] & 0xff) + 0x100, 16).substring(1));

		s.append("] [replyCode=>");
		s.append(NodeCommands.Reply.values()[replyCode].toString());
		s.append("]");

		return s.toString();
	}
}
