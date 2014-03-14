package com.b6w7.eece411.P02.nio;

public interface Process {
	public abstract void checkLocal();
	public abstract void generateOwnerQuery();
	public abstract void generateRequesterReply();
	public abstract void recvOwner();
}
