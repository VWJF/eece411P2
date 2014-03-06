package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

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

	}

	@Override
	public void execute() {
	}
}
