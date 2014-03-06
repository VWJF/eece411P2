package com.b6w7.eece411.P02.multithreaded;

import java.util.concurrent.ConcurrentLinkedQueue;

public class HandlerThread extends Thread implements PostCommand {
	private final ConcurrentLinkedQueue<Command> inQueue = new ConcurrentLinkedQueue<Command>();

	// TODO make private with accessor
	public boolean keepRunning = true;

	public HandlerThread() {
		System.out.println("HandlerThread() constructor");
	}

	@Override
	public void run() {
//		System.out.println("HandlerThread()::run() start");

		Command cmd = null;

		while (keepRunning) {

			cmd = inQueue.poll();

			if (null == cmd) {
				synchronized(inQueue) {
					try {
						System.out.println("HandlerThread()::run() waiting on inQueue");
						inQueue.wait();
					} catch (InterruptedException e) {	/* do nothing. */ }
				}

			} else {
				cmd.execute();
			}
		}
//		System.out.println("HandlerThread()::run() end");
	}

	@Override
	public void post(Command cmd) {
//		System.out.println("HandlerThread()::post() start");
		synchronized(inQueue) {
			inQueue.add(cmd);
			inQueue.notifyAll();
		}
//		System.out.println("HandlerThread()::post() end");
	}
}
