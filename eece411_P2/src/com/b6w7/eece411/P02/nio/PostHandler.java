package com.b6w7.eece411.P02.nio;

import java.util.Collection;

public interface PostHandler {
	
	public abstract void post(Handler cmd);
	public abstract void post(Collection<? extends Handler> h);
	
}
