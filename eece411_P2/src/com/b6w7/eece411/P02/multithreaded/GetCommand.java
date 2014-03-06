package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Reply;

public class GetCommand extends Command {
	private final Socket clientSock;
	
	final ByteBuffer buffer;//= ByteBuffer.allocate(1+32+1024);
	final byte cmd;
	final ByteBuffer key;

	byte replyCode;
	ByteBuffer replyValue;

	// protocol for Request: get command <cmd,key>
	// protocol for Response: <cmd,value>
	public GetCommand(Socket client, byte cmd, ByteBuffer key, Map<String, String> map) {
		// check arguments for correctness
				if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) {
					throw new IllegalArgumentException("key must be 32 bytes for all operations");
				}

				/*if (NodeCommands.CMD_PUT == cmd) {
					if (null == value || value.limit() != NodeCommands.LEN_VALUE_BYTES) 
						throw new IllegalArgumentException("value must be 1024 bytes for PUT operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES
							+NodeCommands.LEN_VALUE_BYTES);

				} else */
				//	if (NodeCommands.CMD_GET == cmd) {
					if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
						throw new IllegalArgumentException("key must be 32 bytes for GET operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES);
					//value = null;

				/*}else if (NodeCommands.CMD_REMOVE == cmd) {
					if (null == key || key.limit() != NodeCommands.LEN_KEY_BYTES) 
						throw new IllegalArgumentException("key must be 32 bytes for Remove operation");

					buffer = ByteBuffer.allocate(
							NodeCommands.LEN_CMD_BYTES
							+NodeCommands.LEN_KEY_BYTES);
					value = null;
				} else {
					buffer = ByteBuffer.allocate(
						NodeCommands.LEN_CMD_BYTES
						+NodeCommands.LEN_KEY_BYTES
						+NodeCommands.LEN_VALUE_BYTES);
						//throw new IllegalArgumentException("Unknown command");
				}
				 */

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

				/*if (null != value) {
					value.rewind();
					buffer.put(value);
				}
				 */
		// check arguments for correctness
		if (client == null) {
			throw new IllegalArgumentException("client socket cannot be null");
		}
		
		this.clientSock = client;
		
	}

	@Override
	public void execute() {
		synchronized(execution_completed){
			ByteBuffer value_of_key =  get();
			if( value_of_key != null ){  
				this.replyCode = (byte) Reply.RPY_SUCCESS.getCode(); 
				this.replyValue.put(value_of_key.array(), 0, 1024); 
			}
			else{
				this.replyCode = (byte) Reply.RPY_INEXISTENT.getCode();
			}
		}
	}
	
	private ByteBuffer get(){
	// TODO: Can be improved (with Error checking, Exception checking, etc.)

		String k = new String( key.array() );
		String val = map.get( k );
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

}
