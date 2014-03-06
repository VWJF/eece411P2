package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;

import com.b6w7.eece411.P02.NodeCommands;

public class UnrecognizedCommand extends Command {
	byte replyCode = NodeCommands.RPY_UNRECOGNIZED_CMD;

	// protocol for Request: get command <cmd,key>
	// protocol for Response: <cmd,value>
	public UnrecognizedCommand() {
		// check arguments for correctness
		this.map = null;
		execution_completed = true;
	}

	@Override
	public void execute() {
		throw new UnsupportedOperationException();
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
	
	@Override
	public String toString(){

		//String k = new String( key.array() );
//		String s = NodeCommands.requestByteArrayToString(buffer.array());
		return Thread.currentThread().getName();
	}
}
