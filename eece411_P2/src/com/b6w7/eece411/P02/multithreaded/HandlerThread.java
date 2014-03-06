package com.b6w7.eece411.P02.multithreaded;

import java.util.concurrent.ConcurrentLinkedQueue;

public class HandlerThread extends Thread implements PostCommand {
	private final ConcurrentLinkedQueue<Command> inQueue = new ConcurrentLinkedQueue<Command>();

	// TODO make private with accessor
	public boolean keepRunning = true;

	public HandlerThread() {

	}

	@Override
	public void run() {

		Command cmd = null;

		while (keepRunning) {

			cmd = inQueue.poll();

			if (null == cmd) {
				synchronized(inQueue) {
					try {
						inQueue.wait();
					} catch (InterruptedException e) {	/* do nothing. */ }
				}

			} else {
				cmd.execute();
			}
		}
	}

	@Override
	public void post(Command cmd) {
		synchronized(inQueue) {
			inQueue.add(cmd);
			inQueue.notifyAll();
		}
	}
}
