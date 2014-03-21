package com.b6w7.eece411.P02.nio;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

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
		for (int i=0; i<this.total_nodes; i++) {
			localTimestampVector.add(i * 1000);
		}
	}
	
	/**
	 * Update the local timestamp vector based on the received vector timestamp 
	 * @param receivedVector
	 */
	public void mergeVector(int[] receivedVector){
		if(receivedVector == null)
			return;
		
		ArrayList<Integer> updateTimestampVector = new ArrayList<Integer>(this.total_nodes);
		
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
		synchronized (localTimestampVector) {
			//behavior on receiving a vectorTimestamp at each node 
			if(IS_DEBUG) System.out.println(" === mergeVector() (localTimestampVector.length=="+localTimestampVector.size()+") (current_node=="+current_node);
			if(IS_DEBUG) {
				StringBuilder s = new StringBuilder();
				s.append(" === MembershipProtocol::mergeVector() [localTimestampVector["+localTimestampVector.size()+"]=>");
				for (Integer element: localTimestampVector)
					s.append(element.toString() + ",");
				s.append("]\n");
				s.append(" === MembershipProtocol::mergeVector()       [receivedVector["+localTimestampVector.size()+"]=>");
				for (int element: receivedVector)
					s.append(element + ",");
				s.append("]");
				System.out.println(s.toString());
			}
			
			int local = localTimestampVector.get(current_node);
			//Implied "success". Executing this method implies that a vector_timestamp was received on the wire. 
			
			int i;
			int update;
			for(i = 0; i < localTimestampVector.size() && i < receivedVector.length; i++){
				update = localTimestampVector.get(i).intValue();
				if(receivedVector[i] > 0 ) {
					update = Math.max(receivedVector[i], update);;
				}
		///***/	update = Math.max(receivedVector[i], localTimestampVector.get(i));
		///***/	localTimestampVector.set(i, update);
				updateTimestampVector.add(new Integer(update));
			}
			if ( localTimestampVector.size() > receivedVector.length ){
				int[] remaining = Arrays.copyOfRange(receivedVector, i, localTimestampVector.size()-1);
				
				updateTimestampVector.addAll(convertArrayToList(remaining));
				//System.arraycopy(remaining, 0, localTimestampVector, i, remaining.length);
			}
			updateTimestampVector.set(current_node, local);
			//localTimestampVector.set(current_node, local);
			localTimestampVector = updateTimestampVector;
			localTimestampVector.trimToSize();
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
		int update;
		
		synchronized (localTimestampVector) {
			update = localTimestampVector.get(current_node).intValue();
			if(update > 0)
				update++;
			localTimestampVector.set(current_node, update);
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			retInteger = new ArrayList<Integer>(localTimestampVector);
			localTimestampVector.trimToSize();
		}
		//if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		if(IS_DEBUG) System.out.println(" === incrementAndGetVector() after update: "+retInteger);

		retInteger.trimToSize();
		int[] backingArray = convertListToArray(retInteger);
		if(IS_DEBUG) System.out.println(" === updateSendVector() backingArray: "+Arrays.toString(backingArray));

		return backingArray;
	}

	private int[] convertListToArray(ArrayList<Integer> retInteger) {
		int update;
		int[] backingArray = new int[total_nodes];
		try {
			Iterator<Integer> iter = retInteger.iterator();
			int i = 0;
			while(iter.hasNext()){
				update = iter.next().intValue();
				backingArray[i++] = update;
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
		Integer timestamp;

		int[] retInt;
		ArrayList<Integer> retInteger;
		synchronized (localTimestampVector) {
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			retInteger = new ArrayList<Integer>(localTimestampVector);
			timestamp = localTimestampVector.get(nodeIndex);
		}
		//if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		if(IS_DEBUG) System.out.println(" === getTimestampVector() "+retInteger);
		
		//return ret;
		retInteger.trimToSize();
		return timestamp.intValue();
	}
	
	public void shutdown(Integer updateIndex){
		//TODO:
		
		if (localTimestampVector == null) 
			System.out.println("		if (localTimestampVector == null) ");
		synchronized (localTimestampVector) {
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			if(updateIndex == null){
				localTimestampVector.set(current_node, -localTimestampVector.get(current_node));
			}
			else{
				localTimestampVector.set(updateIndex.intValue(), -localTimestampVector.get(updateIndex.intValue()));
			}
			int x;
			if(updateIndex == null)
				x = -1;
			else
				x = updateIndex.intValue();
			if(IS_DEBUG) System.out.println(" === shutdownindex "+x);
		}
	}
	
	/**
	 * Helper method to convert int[] to ArrayList<Integer>
	 * @param ints
	 * @return ArrayList<Integer> where ints[x] < 0 are replaced with null entries in the ArrayList
	 */
	private ArrayList<Integer> convertArrayToList(int[] ints){
		ArrayList<Integer> intList = new ArrayList<Integer>(ints.length);
		
		for (int index = 0; index < ints.length; index++)
	    { 
			if (ints[index] < 0){
				intList.add(null);
			}else{
				intList.add(ints[index]);
			}
	    }
		return intList;
	}
}
