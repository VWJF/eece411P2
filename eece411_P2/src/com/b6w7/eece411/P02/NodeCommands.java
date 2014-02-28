package com.b6w7.eece411.P02;

public class NodeCommands {
	final static byte CMD_PUT = 0x1;
	final static byte CMD_GET = 0x2;
	final static byte CMD_REMOVE = 0x3;
	
	final static byte RPY_SUCCESS = 0x0;
	final static byte RPY_INEXISTENT = 0x1;
	final static byte RPY_OUT_OF_SPACE = 0x2;
	final static byte RPY_OVERLOAD = 0x3;
	final static byte RPY_byteERNAL_FAILURE = 0x4;
	final static byte CMD_UNRECOGNIZED = 0x5;
}
