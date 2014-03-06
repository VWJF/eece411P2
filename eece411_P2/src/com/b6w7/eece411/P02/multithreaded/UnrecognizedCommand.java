package com.b6w7.eece411.P02.multithreaded;

import java.net.Socket;
import java.nio.ByteBuffer;

import com.b6w7.eece411.P02.NodeCommands;

public class UnrecognizedCommand extends Command {
	private final Socket clientSock;
	
	final ByteBuffer buffer;//= ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;

	//final Map<String, String> map;
	//final ReplyCommand reply;
	
	byte replyCode = NodeCommands.RPY_UNRECOGNIZED_CMD;
	ByteBuffer replyValue;

	// protocol for Request: get command <cmd,key>
	// protocol for Response: <cmd,value>
	public UnrecognizedCommand() {
		// check arguments for correctness
		
		this.map = null;

		this.clientSock = null;
		
		this.buffer = null;

		this.cmd = (byte) 0;
		this.key = null;

		this.replyValue = null;

		execution_completed = true;

	}

	@Override
	public void execute() {
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
	
	@Override
	public String toString(){

		//String k = new String( key.array() );
		String s = NodeCommands.requestByteArrayToString(buffer.array());
		return Thread.currentThread().getName() + " "+ s.toString();
	}
}
