package com.b6w7.eece411.P02.nio;

public interface Process {
	/** 
	 * check's local database for the given key.
	 * side-effect:
	 * Allocates memory for key and copies from input.
	 * Allocates memory for value and copies from input.
	 * Allocates memory for hashedKey and copies from key.
	 * Allocates memory for output
	 */
	public abstract void checkLocal();

	/**
	 * prepares output for being sent to owner of a key.
	 * side-effect:
	 * Copies into output, 
	 * < cmd > < key > [< value >],
	 * output.position is set to 0, output.limit is set to N.
	 */
	public abstract void generateOwnerQuery();
	
	/**
	 * prepares input for being sent to requester of a key.
	 * Note, we use input because typically input is filled
	 * by a result of reading from a remote node.  This way,
	 * we can prevent a buffer copy for each command.
	 * side-effect:
	 * Copies into input,
	 * < cmd > < key > [< value >],
	 * input.position is set to 0, input.limit is set to N.
	 */
	public abstract void generateRequesterReply();
	
	/**
	 * Receives all bytes from remote node.  May be called
	 * repeatedly until enough bytes are read for a valid reply.
	 */
	public abstract void recvOwner();
}
