package com.b6w7.eece411.P02.multithreaded;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.b6w7.eece411.P02.nio.ServiceReactor;

/**
 * Static class with byte codes for TCP commands with a {@link Node}
 * @author Scott Hazlett
 * @author Ishan Sahay
 */
public class NodeCommands {
	public static final int LEN_CMD_BYTES = 1;
	public static final int LEN_KEY_BYTES = 32;
	public static final int LEN_VALUE_BYTES = 1024;
	public static final int LEN_TIMESTAMP_BYTES = ServiceReactor.nodes.length * 4;
	public static final int REPLICATION_FACTOR = 3;

	// Needed to make a check in ClientInterface whether the "received" command/error code 
	// were among eligible commands.
	// Separated into 2 Enums with Request Commands and Reply Commands
	static public enum Request{
		CMD_UNRECOG((byte)0), 
		CMD_PUT((byte)1), 
		CMD_GET((byte)2), 
		CMD_REMOVE((byte)3), 
		CMD_ANNOUNCEDEATH((byte)4), 
		CMD_NOT_SET((byte)0x20), 
		CMD_TS_PUT((byte)0x21),
		CMD_TS_GET((byte)0x22),
		CMD_TS_REMOVE((byte)0x23),	
		CMD_TS_PUT_BULK((byte)0x24), 
		CMD_TS_REPLICA_PUT((byte)0x25),	
		CMD_TS_REPLICA_REMOVE((byte)0x26),	
		CMD_TS_PUSH((byte)0x27);	
		
		private byte value;

		private Request(byte value) {
			this.value = value;
		}
		public byte getCode() {
			return value;
		}
	/*	// Source:
		// http://howtodoinjava.com/2012/12/07/guide-for-understanding-enum-in-java/
		// Lookup table
		private static final Map<Integer, Request> lookup = new HashMap<Integer, Request>();

		// Populate the lookup table on loading time
		static {
			for (Request s : EnumSet.allOf(Request.class))
				lookup.put(s.getCode(), s);
		}

		// This method can be used for reverse lookup purpose
		public static Request get(int code) {
			return (Request) lookup.get(code);
		}*/
	};

	static public enum Reply{
		RPY_SUCCESS((byte)0),
		RPY_INEXISTENT((byte)1),
		RPY_OUT_OF_SPACE((byte)2),
		RPY_OVERLOAD((byte)3),
		RPY_INTERNAL_FAILURE((byte)4),
		RPY_UNRECOGNIZED((byte)5),
		RPY_NOT_SET((byte)6);

		private byte value;

		private Reply(byte value) {
			this.value = value;
		}
		public byte getCode() {
			return value;
		}
	/*	// Source:
		// http://howtodoinjava.com/2012/12/07/guide-for-understanding-enum-in-java/
		// Lookup table
		private static final Map<Integer, Reply> lookup = new HashMap<Integer, Reply>();

		// Populate the lookup table on loading time
		static {
			for (Reply s : EnumSet.allOf(Reply.class))
				lookup.put(s.getCode(), s);
		}

		// This method can be used for reverse lookup purpose
		public static Reply get(int code) {
			return (Reply) lookup.get(code);
		
		} 
		 */
	};   

	public static Request[] requests = Request.values();
	public static Reply[] replies = Reply.values();

	/*
	 * Given a byte array with the "Request fields" received
	 * returns a the representation of the Request as human readable string 
	 */
	public static String requestByteArrayToString(byte[] recvBytes){
		
		StringBuilder s = new StringBuilder();
		//CMD
		s.append(Integer.toString((recvBytes[0] & 0xff) + 0x100, 16).substring(1));
		s.append(" ");

		//KEY
		try {
			for (int i=LEN_CMD_BYTES; i<(LEN_CMD_BYTES+LEN_KEY_BYTES); i++) {
				s.append(Integer.toString((recvBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
		} catch (IndexOutOfBoundsException e) {
			// do nothing.
			System.out.println("Not enough bytes for KEY field.");
		}
		s.append(" ");

		//Value
		/*for (int i=33; i<dataRead.limit(); i++) {
			s.append(Integer.toString((dataRead.array()[i] & 0xff) + 0x100, 16).substring(1));
		}
		*/
		
		byte valueArrayTemp[] = null;

		try {
			valueArrayTemp = Arrays.copyOfRange(recvBytes, LEN_CMD_BYTES+LEN_KEY_BYTES, LEN_CMD_BYTES+LEN_KEY_BYTES+LEN_VALUE_BYTES);

			// s.append(new String(value.array(), StandardCharsets.UTF_8.displayName()));
			s.append(new String(valueArrayTemp, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			s.append(new String(valueArrayTemp));
		} catch (Exception e) {
			// do nothing.
			// 'Value' does not exist.
		}
		return s.toString();
	}
	
	/*
	 * params: byte array
	 * returns: a String representation of the byte array in hexadecimal notation
	 */
	public static String byteArrayAsString(byte[] array){
		
		if( array ==  null ){
			return "";
		}
		
		StringBuilder s = new StringBuilder();

		for (int i=0; i<(array.length); i++) {
			s.append(Integer.toString((array[i] & 0xff) + 0x100, 16).substring(1));
		}
		
		return s.toString();
	}

	public static byte sanitizeCmd(byte cmd) {
//		if ( cmd <= Request.CMD_TS_REMOVE.getCode() && cmd >= Request.CMD_UNRECOG.getCode() )
//			// so far so good, make sure not an unknown cmd in the middle
//			if (cmd <= Request.CMD_ANNOUNCEDEATH.getCode() || cmd >= Request.CMD_NOT_SET.getCode() )
//				// cmd is valid
//				return cmd;
//		
		boolean isMatch = false;
		for (Request req: Request.values()) {
			if (req.getCode() == cmd) {
				isMatch = true;
				// System.out.println("+-+-NodeCommands:sanitizeCMD "+cmd);
				return req.getCode(); 
				//break MATCH_CMD;
			}
		}
		// command is not valid
		return Request.CMD_UNRECOG.getCode();
	}

	public static NodeCommands.Reply getReplyEnum(byte code) {
		NodeCommands.Reply ret = null;
		for (NodeCommands.Reply rpy: replies) {
			if (rpy.getCode() == code) {
				ret = rpy; 
				break;
			}
		}
		if (ret == null) {
			System.out.println(" *** NodeCommands::getReplyEnum() Received unknown reply 0x" + Integer.toString(0x100 + (code & 0xFF), 16).substring(1));
			ret = NodeCommands.Reply.RPY_UNRECOGNIZED;
		}
		return ret;
	}

	public static NodeCommands.Request getRequestEnum(byte code) {
		NodeCommands.Request ret = null;
		for (NodeCommands.Request req: requests) {
			if (req.getCode() == code) {
				ret = req; 
				break;
			}
		}
		if (ret == null) {
			System.out.println(" *** NodeCommands::getRequestEnum() Received unknown cmd 0x" + Integer.toString(0x100 + (code & 0xFF), 16).substring(1));
			ret = NodeCommands.Request.CMD_UNRECOG;
		}
		return ret;
	}
}
