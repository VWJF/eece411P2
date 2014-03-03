package com.b6w7.eece411.P02;

import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands.Request;
import com.b6w7.eece411.P02.NodeCommands.Reply;


public class Command {
	
	private final Socket clientSock;
	
	final ByteBuffer buffer;//= ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;
	final ByteBuffer value;
	
	byte replyCode;
	ByteBuffer replyValue;
	
	protected static int numElements;
	
	private static final byte ERROR = -1;
	private static final byte MAX_MEMORY = 3;
	private static final Map<String, String> data = new HashMap<String, String>();
	
	private boolean execution_completed = false;

	public Command(Socket client, byte cmd, ByteBuffer key, ByteBuffer value) {
		// check arguments for correctness
				if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) {
					throw new IllegalArgumentException("key must be 32 bytes for all operations");
				}

				if (NodeCommands.CMD_PUT == cmd) {
					if (null == value || value.limit() != NodeCommands.LEN_VALUE_BYTES) 
						throw new IllegalArgumentException("value must be 1024 bytes for PUT operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES
							+NodeCommands.LEN_VALUE_BYTES);

				} else if (NodeCommands.CMD_GET == cmd) {
					if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
						throw new IllegalArgumentException("key must be 32 bytes for GET operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES);
					value = null;

				} else if (NodeCommands.CMD_REMOVE == cmd) {
					if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
						throw new IllegalArgumentException("key must be 32 bytes for Remove operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES);
					value = null;
				} else {
					throw new IllegalArgumentException("Unknown command");
				}



				// Save parameters, and 
				// Place {Cmd, Key, Value} into ByteBuffer 
				// to be ready to be sent down a pipe.  
				this.cmd = cmd;
				this.key = key;
				this.value = value;
			
				buffer.put(cmd);
				key.rewind();
				buffer.put(key);

				if (null != value) {
					value.rewind();
					buffer.put(value);
				}

		// check arguments for correctness
		if (client == null) {
			throw new IllegalArgumentException("client socket cannot be null");
		}
		
		this.clientSock = client;
		
	}
	
	/* Unused */
	private void setRequest( byte cmd, ByteBuffer key, ByteBuffer value){

		// check arguments for correctness
		if (null == key) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (cmd == NodeCommands.CMD_PUT && null == value) {
			throw new IllegalArgumentException("value cannot be null for PUT operation");
		}
		if (key.limit() > 32 || (null != value && value.limit() > 1024)) {
			throw new IllegalArgumentException("key cannot exceed 32 bytes and value cannot exceed 1024 bytes");
		}

		buffer.put(cmd);
		buffer.put(key);
		buffer.put(value);
		
	}

	/*
	 *executes the request received from the client in (command,key,value) 
	 */
	public void executeCommand() {
		
		this.execution_completed = true;
		
		//this.replyValue;
		
		int reply = this.cmd & 0xff;	//Conversion from byte to int.
		Request[] req= Request.values();
		
		if(  reply > req.length ){ /*if(cmd is not one of Enum of possible Request Commands)*/
			this.replyCode = (byte) Reply.CMD_UNRECOGNIZED.getCode();
			return;
		}
		
		switch(req[reply]){
		case CMD_PUT:
			if( put() ){
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_OUT_OF_SPACE.getCode(); 
			}
			break;

		case CMD_GET:
			ByteBuffer value_of_key =  get();
			if( value_of_key != null ){  
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
				//this.replyValue.put(value_of_key.array(), 0, 1024); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_INEXISTENT.getCode();
			}
			break;

		case CMD_REMOVE:
			if( remove() != null ){  
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_INEXISTENT.getCode();
			}
			break;
		
		default:
			break;	
		}
		
	}
	
	/*
	 * returns the appropriate response to be sent to the client for the requested (command,key,value)
	 */
	public ByteBuffer getReply(){
		
		if( execution_completed == false ){
			executeCommand();
		}
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
	
	// Getter
	public Socket getSocket(){
		return this.clientSock;
	}
	
	public static int getNumElements(){
		return data.size();//Command.numElements;
	}
	/*
	 * adds the (key,value) pair into the data structure.
	 * returns true on successful insertion in the data structure, false otherwise.
	 */
	private boolean put(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		StringBuilder s = new StringBuilder();
		/*
		for (int i=0; i<(NodeCommands.LEN_VALUE_BYTES); i++) {
		 
			s.append(Integer.toString((this.value.array()[i] & 0xff) + 0x100, 16).substring(1));
		}
		*/
		
		try {
			s.append(new String(this.value.array(), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			s.append(new String(this.value.array()));
		}
		String k = new String(key.array());
		System.out.println("put (key,value): ("+k+", "+s.toString()+")");
		System.out.println("put key bytes: "+NodeCommands.byteArrayAsString(key.array()) );
		System.out.println("put value bytes: "+NodeCommands.byteArrayAsString((s.toString().getBytes())) );

		//if(data.size() < MAX_MEMORY){
			data.put(new String(key.array()), new String(this.value.array()) );
		//	Command.numElements++;
			return true;
		//}
		//return false;
	}
	
	/*
	 *
	 * returns the value for the requested key if it is found, null otherwise.
	 */
	private ByteBuffer get(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		
		String k = new String( key.array() );
		String val = data.get( k );
			System.out.println("get (key, value): ("+k+", "+val+")");
			try {
				System.out.println("get key bytes: "+NodeCommands.byteArrayAsString(key.array()) );
				if(val != null){
					System.out.println("get value bytes: "+NodeCommands.byteArrayAsString(val.getBytes("UTF-8")) );
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return (val != null) ? this.replyValue = ByteBuffer.wrap(val.getBytes()) : null;
		
	}
	
	/*
	 * removes the (key,value) pair from the data structure. 
	 * returns the value if the key was present in the structure, null otherwise.
	 */
	private String remove(){
		// TODO: Can be improved (with Error checking, Exception checking, etc.)
		String removed = data.remove(new String(key.array()));
		//if(removed == null)
		//	Command.numElements--;
		
		return removed;
	}
	
	public byte getCmd(){
		return this.cmd;
	}
	public ByteBuffer getKey(){
		return this.key;
	}
	public ByteBuffer getValue(){
		return this.value;
	}
}
