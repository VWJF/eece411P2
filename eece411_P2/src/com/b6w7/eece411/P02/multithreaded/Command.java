package com.b6w7.eece411.P02.multithreaded;

import java.util.Map;

public abstract class Command {
	
	protected static final int MAX_MEMORY = 40000;
	
	public Map<ByteArrayWrapper, byte[]> map;// = new HashMap<String, String>();

	protected Boolean execution_completed = false;
	
	public static final int LEN_TO_STRING_OF_KEY = 5;
	public static final int LEN_TO_STRING_OF_VAL = 5;

	public abstract void execute();
	
	public abstract byte[] getReply();
}
