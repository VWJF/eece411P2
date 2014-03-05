package com.b6w7.eece411.P02.multithreaded;

public interface Command {
	static final byte MAX_MEMORY = 3;

	public abstract void execute();
}
