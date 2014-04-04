package com.b6w7.eece411.P02.nio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.HandlerThread;
import com.b6w7.eece411.P02.multithreaded.PostCommand;

public class ReplicaThread extends Thread implements PostCommand<Handler> {
	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);
	private final ConcurrentLinkedQueue<Handler> inQueue = new ConcurrentLinkedQueue<Handler>();
	private final HandlerThread dbHandler;

	public boolean keepRunning = true;

	public ReplicaThread(HandlerThread dbHandler) {
		this.dbHandler = dbHandler;
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
				dbHandler.post(msg);
				log.debug(" --- ReplicaThread::run() Complete: {}", msg);
			}
		}
		log.debug(" --- ReplicaThread()::run() end");
	}

	/**
	 * Given a replica Handler, add to a queue for processing.
	 * @param h
	 */
	@Override
	public void post(Handler h) {
		synchronized(inQueue) {
			inQueue.add(h);
			inQueue.notifyAll();
		}
	}

	/**
	 * Given several replica Handler, add to a queue for processing.
	 * assume: The necessary number of replicas are instantiated: REPLICATION_FACTOR
	 * @param multipleHandlers
	 */
	@Override
	public void post(Collection<? extends Handler> multipleHandlers) {
		synchronized(inQueue) {
			inQueue.addAll(multipleHandlers);
			inQueue.notifyAll();
		}
	}

	/**
	 * Verify and Remove all instances of unique Handler from the replica processing queue.
	 * @param h
	 */
	public boolean removeReplica(Handler h){
		//TODO: Where will REPLICATION_FACTOR number of Handlers be instantiated?

		List<Handler> reps = new ArrayList<Handler>();
		Collections.addAll(reps, h);

		//return replicaQueue.removeAll(reps); // same as verifyReplica(List<Handler>)

		return removeReplica(reps);
	}

	/**
	 * Verify and Remove the several instances of a unique Handler(s) from the replica processing queue.
	 * @param multipleHandler
	 */
	public boolean removeReplica(List<? extends Handler> multipleHandler){
		//TODO: Where will REPLICATION_FACTOR number of Handlers be instantiated?

		return inQueue.removeAll(multipleHandler);
	}

	@Override
	public void kill() {
		keepRunning = false;
		synchronized(inQueue) {
			inQueue.notifyAll();
		}
	}
}
