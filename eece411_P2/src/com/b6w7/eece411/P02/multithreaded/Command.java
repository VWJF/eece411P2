package com.b6w7.eece411.P02.multithreaded;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract class that contains the information for processing one command received over TCP.
 * One TCP connection with multiple commands will spawn multiple copies of this.
 */
public abstract class Command {

	/**
	 * The upper bound on the maximum number of entries in the Distributed Hash Table
	 */
	protected static final int MAX_MEMORY = 40000;

	/**
	 * A reference to the local (sub)portion of the Distributed Hash Table
	 */
	public Map<ByteArrayWrapper, byte[]> map;
		
	// toString parameters
	public static final int LEN_TO_STRING_OF_KEY = 5;
	public static final int LEN_TO_STRING_OF_VAL = 5;

	// debug variables
	public static AtomicInteger totalCompleted = new AtomicInteger(0);
	protected boolean IS_VERBOSE = true;
	
	// abstact class methods to be overriden
	public abstract void execute();

	
	// to become obsolete ...
	protected boolean execution_completed = false;
	protected final Object execution_completed_sem = new Object(); 
	public abstract byte[] getReply();
}
