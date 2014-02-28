package com.b6w7.eece411.P02;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands.Reply;

public class NodeCommands {
	final static byte CMD_PUT = 0x1;
	final static byte CMD_GET = 0x2;
	final static byte CMD_REMOVE = 0x3;
	
	final static byte RPY_SUCCESS = 0x0;
	final static byte RPY_INEXISTENT = 0x1;
	final static byte RPY_OUT_OF_SPACE = 0x2;
	final static byte RPY_OVERLOAD = 0x3;
	final static byte RPY_INTERNAL_FAILURE = 0x4;
	final static byte CMD_UNRECOGNIZED = 0x5;

	// Need to make a check in ClientInterface whether the "received" command/error code 
	// were among eligible commands.
	// Separated into 2 Enums with Request Commands and Reply Commands
	static public enum Request{
		CMD_PUT(1), 
		CMD_GET(2), 
		CMD_REMOVE(3);
		
		private int value;

		private Request(int value) {
			this.value = value;
		}
		public int getCode() {
			return value;
		}
		// Source:
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
		}
	};
		
	static public enum Reply{
		RPY_SUCCESS(0),
		RPY_INEXISTENT(1),
		RPY_OUT_OF_SPACE(2),
		RPY_OVERLOAD(3),
		RPY_INTERNAL_FAILURE(4),
		CMD_UNRECOGNIZED(5);

		private int value;

		private Reply(int value) {
			this.value = value;
		}
		public int getCode() {
			return value;
		}
		// Source:
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
	};   
}
