package com.b6w7.eece411.P02.multithreaded;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class Command {
	
	protected static final byte MAX_MEMORY = 5;
	
	protected Map<String, String> map;// = new HashMap<String, String>();

	protected Boolean execution_completed = false;

	public abstract void execute();
	
	public abstract ByteBuffer getReply();
}
