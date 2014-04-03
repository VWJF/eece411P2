package com.b6w7.eece411.P02.nio;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.PostCommand;
import com.b6w7.eece411.P02.nio.ServiceReactor;

public class ReplicaThread extends Thread implements PostCommand<Handler> {
	private final ConcurrentLinkedQueue<Handler> inQueue = new ConcurrentLinkedQueue<Handler>();

	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);

	public boolean keepRunning = true;

	public ReplicaThread() {
	}

	@Override
	public void run() {
		Handler msg = null;

		while (keepRunning) {

			msg = inQueue.poll();

			if (null == msg) {
				synchronized(inQueue) {
					try {
						log.debug(" --- ReplicaThread()::run() waiting on inQueue");
						inQueue.wait();
					} catch (InterruptedException e) {	/* do nothing. */ }
				}

			} else {
				log.debug(" --- ReplicaThread::run() Issuing:  {}", msg);
				msg.execute();
				log.debug(" --- ReplicaThread::run() Complete: {}", msg);
			}
		}
		log.debug(" --- ReplicaThread()::run() end");
	}

	@Override
	public void post(Handler msg) {
		synchronized(inQueue) {
			inQueue.add(msg);
			inQueue.notifyAll();
		}
	}

	@Override
	public void kill() {
		keepRunning = false;
		synchronized(inQueue) {
			inQueue.notifyAll();
		}
	}
}
