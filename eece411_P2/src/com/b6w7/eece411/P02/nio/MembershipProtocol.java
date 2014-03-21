package com.b6w7.eece411.P02.nio;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;

public class MembershipProtocol {
	/**
	 * The MembershipProtocol class can have a run() & execute().
	 * run() would be used to send the local vectortimestamp to a remote node [currently method sendVector()]
	 * execute() would be used to update the local vectortimestamp with 
	 * 		the vector received from a remote node [currently method receiveVector()]
	 */
	private final int total_nodes;
	private int current_node;
	private ArrayList<Integer> localTimestampVector; //TODO: changed from int[] to Integer[]
	
	public static boolean IS_DEBUG = true;


	public MembershipProtocol(int current_node, int total_nodes) {
		this.current_node = current_node;
		this.total_nodes = total_nodes;
		this.localTimestampVector = new ArrayList<Integer>(this.total_nodes);
		
		// TODO debugging starting values:
		for (int i=0; i<localTimestampVector.size(); i++) {
			localTimestampVector.set(i, i * 1000);
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
			if(IS_DEBUG) System.out.println(" === mergeVector() (localTimestampVector.length=="+localTimestampVector.size()+") (current_node=="+current_node);
			int local = localTimestampVector.get(current_node);
			//Implied "success". Executing this method implies that a vector_timestamp was received on the wire. 
			//if (success){
			int i;
			for(i = 0; i < localTimestampVector.size() && i < receivedVector.length; i++){
				int update = Math.max(receivedVector[i], localTimestampVector.get(i));
				localTimestampVector.set(i, update); 
			}
			if ( localTimestampVector.size() > receivedVector.length ){
				int[] remaining = Arrays.copyOfRange(receivedVector, i, localTimestampVector.size()-1);
				System.arraycopy(remaining, 0, localTimestampVector, i, remaining.length);
			}
			//else if (i == receivedOnWire.length && i < localTimestampVector.length)

			localTimestampVector.set(current_node, local);
			//	wait(waittime);

			if(IS_DEBUG) System.out.println(" === mergeVector() (localIndex="+local+") received vect: "+Arrays.toString(receivedVector));
			if(IS_DEBUG) System.out.println(" === mergeVector() (localIndex="+local+") after merging: "+localTimestampVector);
		}
	}

	/**
	 * Preparing the Vector Timestamp to be sent to a remote node.
	 * @return
	 */
	public int[] incrementAndGetVector(){
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
		int[] retInt;
		ArrayList<Integer> retInteger;

		synchronized (localTimestampVector) {
			int update = localTimestampVector.get(current_node).intValue();
			localTimestampVector.set(current_node, update++);
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			retInteger = new ArrayList<Integer>(localTimestampVector);
		}
		//if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+retInteger);

		int[] backingArray = new int[total_nodes];
		try {
			Iterator<Integer> iter = retInteger.iterator();
			int i = 0;
			while(iter.hasNext()){
				backingArray[i] = iter.next().intValue();
			}
		} catch (IndexOutOfBoundsException iob) {
			iob.printStackTrace();
			//Nothing.
		}
		return backingArray;
	}
	
	/**
	 * Accessor for the local Timestamp Vector.
	 * @return
	 */
	public int getTimestamp(int nodeIndex){
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
		int timestamp;

		int[] retInt;
		ArrayList<Integer> retInteger;
		synchronized (localTimestampVector) {
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			retInteger = new ArrayList<Integer>(localTimestampVector);
			timestamp = localTimestampVector.get(nodeIndex);
		}
		//if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+retInteger);
		
		//return ret;
		return timestamp;
	}
	
	public void shutdown(Integer update){
		//TODO:
		synchronized (localTimestampVector) {
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			localTimestampVector.set(current_node, update);
		}
	}
	
}