package com.b6w7.eece411.P02.nio;

import java.util.Arrays;

public class MembershipProtocol {
	/**
	 * The MembershipProtocol class can have a run() & execute().
	 * run() would be used to send the local vectortimestamp to a remote node [currently method sendVector()]
	 * execute() would be used to update the local vectortimestamp with 
	 * 		the vector received from a remote node [currently method receiveVector()]
	 */
	private final int total_nodes;
	private int current_node;
	private int[] localTimestampVector;
	
	public static boolean IS_DEBUG = true;


	public MembershipProtocol(int current_node, int total_nodes) {
		this.current_node = current_node;
		this.total_nodes = total_nodes;
		this.localTimestampVector = new int[this.total_nodes];
		
		// TODO debugging starting values:
		for (int i=0; i<localTimestampVector.length; i++) {
			localTimestampVector[i] = i * 1000;
		}
	}
	
	/**
	 * Update the local timestamp vector based on the received vector timestamp 
	 * @param receivedVector
	 */
	public void mergeVector(int[] receivedVector){
		if(receivedVector == null)
			return;
		
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
		synchronized (localTimestampVector) {
			//behavior on receiving a vectorTimestamp at each node 
			int local = localTimestampVector[current_node];
			//Implied "success". Executing this method implies that a vector_timestamp was received on the wire. 
			//if (success){
			int i;
			for(i = 0; i < localTimestampVector.length && i < receivedVector.length; i++){
				localTimestampVector[i] = Math.max(receivedVector[i], localTimestampVector[i]); 
			}
			if ( localTimestampVector.length > receivedVector.length ){
				int[] remaining = Arrays.copyOfRange(receivedVector, i, localTimestampVector.length-1);
				System.arraycopy(remaining, 0, localTimestampVector, i, remaining.length);
			}
			//else if (i == receivedOnWire.length && i < localTimestampVector.length)

			localTimestampVector[current_node] = local;
			//	wait(waittime);

			if(IS_DEBUG) System.out.println(" === mergeVector() (localIndex="+local+") received vect: "+Arrays.toString(receivedVector));
			if(IS_DEBUG) System.out.println(" === mergeVector() (localIndex="+local+") after merging: "+Arrays.toString(localTimestampVector));
		}
	}

	/**
	 * Preparing the Vector Timestamp to be sent to a remote node.
	 * @return
	 */
	public int[] updateSendVector(){
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
		int[] ret;
		synchronized (localTimestampVector) {
			localTimestampVector[current_node]++;
			ret = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
		}
		if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		return ret;
	}
	
}