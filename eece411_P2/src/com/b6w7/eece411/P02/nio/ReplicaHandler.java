package com.b6w7.eece411.P02.nio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ReplicaHandler implements PostHandler {
	private ConcurrentLinkedQueue<Handler> replicaQueue;

	public ReplicaHandler() {
		replicaQueue = new ConcurrentLinkedQueue<Handler>();
	}
	
	 /**
	 * Given a replica Handler, add to a queue for processing.
 	 * @param h
	 */
	@Override
	public void post(Handler h) {
		// TODO 
		// Consider verifying use of synchronized(..){....} in HandlerThread.post(Command cmd)
		replicaQueue.add(h);
		replicaQueue.notifyAll();
	}
	
	/**
	 * Given several replica Handler, add to a queue for processing.
	 * assume: The necessary number of replicas are instantiated: REPLICATION_FACTOR
	 * @param multipleHandlers
	 */
	@Override
	public void post(Collection<? extends Handler> multipleHandlers) {
		// TODO 
		// Consider verifying use of synchronized(..){....} in HandlerThread.post(Command cmd)
		replicaQueue.addAll(multipleHandlers);
		replicaQueue.notifyAll();
	}

	/**
	 * Verify and Remove all instances of unique Handler from the replica processing queue.
	 * @param h
	 */
	public boolean verifyReplica(Handler h){
		//TODO: Where will REPLICATION_FACTOR number of Handlers be instantiated?
		
		List<Handler> reps = new ArrayList<Handler>();
		Collections.addAll(reps, h);
		
		//return replicaQueue.removeAll(reps); // same as verifyReplica(List<Handler>)
		
		return verifyReplica(reps);
	}
	
	/**
	 * Verify and Remove the several instances of a unique Handler(s) from the replica processing queue.
	 * @param multipleHandler
	 */
	public boolean verifyReplica(List<? extends Handler> multipleHandler){
		//TODO: Where will REPLICATION_FACTOR number of Handlers be instantiated?
		
		return replicaQueue.removeAll(multipleHandler);
	}
}
