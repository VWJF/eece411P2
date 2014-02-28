package com.b6w7.eece411.P02;

/**
 * Static class with byte codes for TCP commands with a {@link Node}
 * @author Scott Hazlett
 * @author Ishan Sahay
 */
public class NodeCommands {
	public static final byte CMD_PUT = 0x1;
	public static final byte CMD_GET = 0x2;
	public static final byte CMD_REMOVE = 0x3;
	
	public static final byte RPY_SUCCESS = 0x0;
	public static final byte RPY_INEXISTENT = 0x1;
	public static final byte RPY_OUT_OF_SPACE = 0x2;
	public static final byte RPY_OVERLOAD = 0x3;
	public static final byte RPY_INTERNAL_FAILURE = 0x4;
	public static final byte RPY_UNRECOGNIZED_CMD = 0x5;
	
	public static final int LEN_CMD_BYTES = 1;
	public static final int LEN_KEY_BYTES = 32;
	public static final int LEN_VALUE_BYTES = 1024;
}
