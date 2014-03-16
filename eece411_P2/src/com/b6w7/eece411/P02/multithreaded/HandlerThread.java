package com.b6w7.eece411.P02.multithreaded;

import java.util.concurrent.ConcurrentLinkedQueue;

public class HandlerThread extends Thread implements PostCommand {
	private final ConcurrentLinkedQueue<Command> inQueue = new ConcurrentLinkedQueue<Command>();

	// TODO make private with accessor
	public boolean keepRunning = true;
	private boolean IS_VERBOSE = Command.IS_VERBOSE;

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
						if (IS_VERBOSE) System.out.println("HandlerThread()::run() waiting on inQueue");
						inQueue.wait();
					} catch (InterruptedException e) {	/* do nothing. */ }
				}

			} else {
				if (IS_VERBOSE) System.out.println("Issuing:  "+cmd);
				cmd.execute();
				System.out.println("Complete: "+cmd+ " totalCompleted=="+Command.totalCompleted.incrementAndGet()+" map.size=="+cmd.map.size());
			}
		}
		//TODP: Not reached ......??
		System.out.println("HandlerThread()::run() end");
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
