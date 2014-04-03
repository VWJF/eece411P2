package com.b6w7.eece411.P02.multithreaded;

public interface PostCommand <Type>{
	
	public abstract void post(Type msg);

	public abstract void kill();
}
