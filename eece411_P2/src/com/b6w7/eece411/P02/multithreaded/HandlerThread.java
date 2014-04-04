package com.b6w7.eece411.P02.multithreaded;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.nio.ServiceReactor;

public class HandlerThread extends Thread implements PostCommand<Command> {
	private final ConcurrentLinkedQueue<Command> inQueue = new ConcurrentLinkedQueue<Command>();

	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);

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
						log.debug(" --- HandlerThread()::run() waiting on inQueue");
						inQueue.wait();
					} catch (InterruptedException e) {	/* do nothing. */ }
				}

			} else {
				log.debug(" --- HandlerThread::run() Issuing:  {}", cmd);
				cmd.execute();
				log.debug(" --- HandlerThread::run() Complete: {} totalCompleted=={} map.size=={}", cmd, Command.totalCompleted.incrementAndGet(), cmd.map.size());
			}
		}
		//TODO: Not reached ......??
		log.debug(" --- HandlerThread()::run() end");
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

	@Override
	public void kill() {
		keepRunning = false;
		synchronized(inQueue) {
			inQueue.notifyAll();
		}
	}

	@Override
	public void post(Collection<? extends Command> multipleHandlers) {
		throw new UnsupportedOperationException();
	}
}
