package com.b6w7.eece411.P02.multithreaded;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Command {
	
	protected static final int MAX_MEMORY = 40000;

	public static AtomicInteger atom = new AtomicInteger(0);
	
	public Map<ByteArrayWrapper, byte[]> map;// = new HashMap<String, String>();

	protected boolean execution_completed = false;
	protected final Object execution_completed_sem = new Object(); 
	
	public static final int LEN_TO_STRING_OF_KEY = 5;
	public static final int LEN_TO_STRING_OF_VAL = 5;

	protected boolean IS_VERBOSE = false;

	public abstract void execute();
	
	public abstract byte[] getReply();
}
