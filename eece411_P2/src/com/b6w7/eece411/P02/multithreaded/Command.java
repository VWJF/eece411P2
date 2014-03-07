package com.b6w7.eece411.P02.multithreaded;

import java.util.Map;

public abstract class Command {
	
	protected static final int MAX_MEMORY = 30;
	
	protected Map<ByteArrayWrapper, byte[]> map;// = new HashMap<String, String>();

	protected Boolean execution_completed = false;

	public abstract void execute();
	
	public abstract byte[] getReply();
}
