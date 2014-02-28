package com.b6w7.eece411.P02;

public class NodeCommands {
	static int CMD_PUT = 0x1;
	static int CMD_GET = 0x2;
	static int CMD_REMOVE = 0x3;
	
	static int RPY_SUCCESS = 0x0;
	static int RPY_INEXISTENT = 0x1;
	static int RPY_OUT_OF_SPACE = 0x2;
	static int RPY_OVERLOAD = 0x3;
	static int RPY_INTERNAL_FAILURE = 0x4;
	static int CMD_UNRECOGNIZED = 0x5;
}
