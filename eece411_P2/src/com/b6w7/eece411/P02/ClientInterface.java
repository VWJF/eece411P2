package com.b6w7.eece411.P02;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands.Request;
import com.b6w7.eece411.P02.NodeCommands.Reply;


public class ClientInterface {
	
	private final Socket clientSock;
	
	final ByteBuffer buffer = ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;
	final ByteBuffer value;
	
	byte replyCode;
	ByteBuffer replyValue;
	
	private static final byte ERROR = -1;
	private static final byte MAX_MEMORY = 64;
	private static final Map<String, String> data = new HashMap<String, String>();
	
	private boolean execution_completed = false;

	public ClientInterface(Socket client, byte cmd, ByteBuffer key, ByteBuffer value) {
		
		// check arguments for correctness
		if (client == null) {
			throw new IllegalArgumentException("client socket cannot be null");
		}
		if (null == key) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (key.equals(NodeCommands.CMD_PUT) && null == value) {
			throw new IllegalArgumentException("value cannot be null for PUT operation");
		}
		if (key.limit() > 32 || (null != value && value.limit() > 1024)) {
			throw new IllegalArgumentException("key cannot exceed 32 bytes and value cannot exceed 1024 bytes");
		}

		buffer.put(cmd);
		buffer.put(key);
		buffer.put(value);
		this.cmd = cmd;
		this.key = key;
		this.value = value;
		this.clientSock = client;
		
		this.replyValue = null;
		this.replyCode = ERROR;
		
	}

	/*
	 *executes the request received from the client in (command,key,value) 
	 */
	public void executeCommand() {
		
		this.execution_completed = true;
		
		this.replyValue.clear();
		
		int reply = this.cmd & 0xff;	//Conversion from byte to int.

		if( Request.get(reply) == null ){ /*if(cmd is not one of Enum of possible Request Commands)*/
			this.replyCode = (byte) Reply.CMD_UNRECOGNIZED.getCode();
			return;
		}
		
		switch(this.cmd){
		case NodeCommands.CMD_PUT:
			if( put() ){
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_OUT_OF_SPACE.getCode(); 
			}
			break;

		case NodeCommands.CMD_GET:
			ByteBuffer value_of_key =  get();
			if( value_of_key != null ){  
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
				this.replyValue.put(value_of_key.array(), 0, 1024); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_INEXISTENT.getCode();
			}
			break;

		case NodeCommands.CMD_REMOVE:
			if( remove() != null ){  
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_INEXISTENT.getCode();
			}
			break;
		
		default:
			this.replyCode = (byte) Reply.CMD_UNRECOGNIZED.getCode(); 
		}
		
	}
	
	/*
	 * returns the appropriate response to be sent to the client for the requested (command,key,value)
	 */
	public ByteBuffer getReply(){
		
		if( execution_completed == false ){
			executeCommand();
		}
		
		ByteBuffer response = ByteBuffer.allocate( 1 + replyValue.capacity());
		response.put(replyCode);
		response.put(replyValue);
		return response;
	}
	
	// Setter
	public Socket getSocket(){
		return this.clientSock;
	}
	
	/*
	 * adds the (key,value) pair into the data structure.
	 * returns true on successful insertion in the data structure, false otherwise.
	 */
	private boolean put(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		if(data.size() < MAX_MEMORY){
			data.put(new String(key.array()), new String(this.value.array()) );
			return true;
		}
		return false;
	}
	
	/*
	 *
	 * returns the value for the requested key if it is found, null otherwise.
	 */
	private ByteBuffer get(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)

		String val = data.get(new String(key.array()));	
		return (val != null) ? this.replyValue = ByteBuffer.wrap(val.getBytes()) : null;
	}
	
	/*
	 * removes the (key,value) pair from the data structure. 
	 * returns the value if the key was present in the structure, null otherwise.
	 */
	private String remove(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		return data.remove(new String(key.array()));
	}
}
