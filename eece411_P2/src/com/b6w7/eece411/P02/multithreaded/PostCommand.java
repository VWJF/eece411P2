package com.b6w7.eece411.P02.multithreaded;

import java.util.Collection;

public interface PostCommand <Type>{
	
	public abstract void post(Type msg);

	public abstract void kill();

	void post(Collection<? extends Type> multipleHandlers);
}
