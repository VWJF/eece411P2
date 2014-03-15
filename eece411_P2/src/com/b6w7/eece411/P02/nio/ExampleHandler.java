package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

final class ExampleHandler implements Runnable { 
	final SocketChannel socket;
	final SelectionKey sk;
	ByteBuffer input = ByteBuffer.allocateDirect(2048);
	ByteBuffer output = ByteBuffer.allocate(2048);
	static final int READING = 0, SENDING = 1;
	int state = READING;

	ExampleHandler(Selector sel, SocketChannel c) 
			throws IOException { 
		socket = c; 
		c.configureBlocking(false);
		// Optionally try first read now
		sk = socket.register(sel, 0);
		sk.attach(this);
		sk.interestOps(SelectionKey.OP_READ);
		sel.wakeup();
	}

	boolean inputIsComplete() { return true; }
	boolean outputIsComplete() { return true; }
	void process() { /* ... */ }

	@Override
	public void run() {
		try { 
			if (state == READING) read(); 
			else if (state == SENDING) send();
		} catch (IOException ex) { /* ... */ }
	} 

	void read() throws IOException {
		socket.read(input);
		if (inputIsComplete()) {
			process(); 
			state = SENDING; 
			// Normally also do first write now
			sk.interestOps(SelectionKey.OP_WRITE);
		}
	}
	void send() throws IOException {
		socket.write(output);
		if (outputIsComplete()) sk.cancel();
	} 

}