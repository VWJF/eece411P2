package com.b6w7.eece411.P02.nio;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MembershipProtocol {
	/**
	 * The MembershipProtocol class can have a run() & execute().
	 * run() would be used to send the local vectortimestamp to a remote node [currently method sendVector()]
	 * execute() would be used to update the local vectortimestamp with 
	 * 		the vector received from a remote node [currently method receiveVector()]
	 */
	private final int total_nodes;
	private final int current_node;
	private ArrayList<Integer> localTimestampVector; //TODO: changed from int[] to Integer[]
	
	private static boolean IS_DEBUG = true; //true: System.out enabled, false: disabled

	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);


	public MembershipProtocol(int current_node, int total_nodes) {
		this.current_node = current_node;
		this.total_nodes = total_nodes;
		this.localTimestampVector = new ArrayList<Integer>(this.total_nodes);
		
		for (int i=0; i<this.total_nodes; i++) {
			localTimestampVector.add(1);
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
//		synchronized (localTimestampVector) {
			//behavior on receiving a vectorTimestamp at each node 
			log.debug(" === mergeVector() (localTimestampVector.length=={}) (current_node=={})", localTimestampVector.size(), current_node);
			
//			if(IS_DEBUG) {
//				StringBuilder s = new StringBuilder();
//				s.append(" === MembershipProtocol::mergeVector() [localTimestampVector["+localTimestampVector.size()+"]=>");
//				for (Integer element: localTimestampVector)
//					s.append(element.toString() + ",");
//				s.append("]\n");
//				s.append(" === MembershipProtocol::mergeVector() [receivedVector["+localTimestampVector.size()+"]=>");
//				for (int element: receivedVector)
//					s.append(element + ",");
//				s.append("]");
//				System.out.println(s.toString());
//			}
			
			int local = localTimestampVector.get(current_node);
			log.debug(" === mergeVector() (localIndex={}) received vect: {}", local, Arrays.toString(receivedVector));
			log.debug(" === mergeVector() (localIndex={}) local vect   : {}", local, Arrays.toString(convertListToArray(localTimestampVector)));

			//Implied "success". Executing this method implies that a vector_timestamp was received on the wire. 
			
			int i, localView, remoteView;
			int update;
			if (IS_DEBUG) 
				if (localTimestampVector.size() != receivedVector.length) 
					System.out.println(" ### MembershipProtocol::mergeVector() receivedVector.length==" + receivedVector.length);

			for(i = 0; i < localTimestampVector.size(); i++){

				localView = localTimestampVector.get(i).intValue();
				remoteView = receivedVector[i];
				//localView = update;
				update = localView;
				if(Math.abs(localView) == Math.abs(remoteView)){
					//Timestamps contain identical entries (in magnitude), then update with the entry that is negative. 
					if (localView < remoteView){
						 update = localView;
					 }else{
						 update = remoteView;
					 }
				}
				else if(Math.abs(localView) < Math.abs(remoteView)) { 
					update = remoteView;
				}
				//update = Math.max(receivedVector[i], update);;
				///***/	update = Math.max(receivedVector[i], localTimestampVector.get(i));
				///***/	localTimestampVector.set(i, update);
				updateTimestampVector.add(new Integer(update));
				//			}
				//			if ( localTimestampVector.size() > receivedVector.length ){
				//				int[] remaining = Arrays.copyOfRange(receivedVector, i, localTimestampVector.size()-1);
				//				
				//				updateTimestampVector.addAll(convertArrayToList(remaining));
				//				//System.arraycopy(remaining, 0, localTimestampVector, i, remaining.length);
			}
			updateTimestampVector.set(current_node, local);
			//localTimestampVector.set(current_node, local);
			localTimestampVector = updateTimestampVector;
			localTimestampVector.trimToSize();
			//	wait(waittime);

			log.debug(" === mergeVector() (localIndex={}) after merging: {}", local, localTimestampVector);
	
	}

	/**
	 * Preparing the Vector Timestamp to be sent to a remote node.
	 * @return
	 */
	public int[] incrementAndGetVector(){
		int[] retInt;
		ArrayList<Integer> retInteger;
		int update;
		
//		synchronized (localTimestampVector) {
			update = localTimestampVector.get(current_node).intValue();
			if(update > 0)
				update++;
			localTimestampVector.set(current_node, update);
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			retInteger = new ArrayList<Integer>(localTimestampVector);
			localTimestampVector.trimToSize();
//		}
		//if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		log.debug(" === incrementAndGetVector() after update: {}", retInteger);

		retInteger.trimToSize();
		int[] backingArray = convertListToArray(retInteger);
		log.debug(" === incrementAndGetVector() backingArray: {}", Arrays.toString(backingArray));

		return backingArray;
	}

	/**
	 * Return a random index in the range of localtimestampVector that does not match the current_node
	 * @return
	 */
	public Integer getRandomIndex(){
		//To obtain a unique random number to use as indices fo localtimestampvector.
		//Create a list of all entries in the localtimestampvector. 
		//Use Collections.shuffle() to randomize the list.
		//The random index will be an entry from the list.
		ArrayList<Integer> rand = new ArrayList<Integer>(this.total_nodes);
		int i = 0;
		for(i = 0; i < total_nodes; i++){
			rand.add(i);
		}
		Collections.shuffle(rand);
		
		log.trace(" === getRandomIndex() prior");
		log.trace(" === Random generator: shuffle: {}", rand);
		
		int test, randomIndex = current_node;
		Integer r;
		boolean validRandomFound = false;
		i = total_nodes;
		try{
			do{
				r = rand.remove(0);
				if(r == null){
					validRandomFound = false;
					break;
				}
				randomIndex = r.intValue();
				test = localTimestampVector.get(randomIndex);
				if(test > 0){
					validRandomFound = true;
					break;
				}
			}while(  (--i) != 0  );
		}catch(NullPointerException npe){
			//A valid entry does not exist.
			//npe.printStackTrace();
			if(IS_DEBUG) System.out.println(" === getRandomIndex() NullPointerExcept: "+npe.getLocalizedMessage());
			randomIndex = current_node;
		}catch(IndexOutOfBoundsException iob){
			//A valid entry does not exist.
			//iob.printStackTrace();
			if(IS_DEBUG) System.out.println(" === getRandomIndex() IndexOutBndExcept: "+iob.getLocalizedMessage());
			randomIndex = current_node;
		}
		log.debug(" === getRandomIndex() post: {}", randomIndex);

		if(randomIndex == current_node )
			return null;
		 
		return new Integer(randomIndex);
	}
	/**
	 * Helper method to obtain a int[] from an ArrayList<Integer>.
	 * @param retInteger
	 * @return
	 */
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
	 * Accessor for an index(node) the local Timestamp Vector.
	 * @return
	 */
	public int getTimestamp(int nodeIndex){
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
		Integer timestamp;

		int[] retInt;
		ArrayList<Integer> retInteger;
//		synchronized (localTimestampVector) {
			//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
			retInteger = new ArrayList<Integer>(localTimestampVector);
			timestamp = localTimestampVector.get(nodeIndex);
//		}
		//if(IS_DEBUG) System.out.println(" === updateSendVector() after update: "+Arrays.toString(ret));
		log.trace(" === getTimestampVector() {}", retInteger);
		
		//return ret;
		retInteger.trimToSize();
		return timestamp.intValue();
	}
	
	public void shutdown(Integer updateIndex){
		//TODO:
		
		if (localTimestampVector == null) 
			log.error(" ### localTimestampVector is null");

		//retInt = Arrays.copyOf(localTimestampVector, localTimestampVector.length);
		if(updateIndex == null){
			localTimestampVector.set(current_node, -Math.abs(localTimestampVector.get(current_node)));
		}
		else{
			localTimestampVector.set(updateIndex.intValue(), -Math.abs(localTimestampVector.get(updateIndex.intValue())));
		}
		int shutdownIndex;
		if(updateIndex == null)
			shutdownIndex = -1;
		else
			shutdownIndex = updateIndex.intValue();
		log.debug(" === shutdownindex {} {}", shutdownIndex, Arrays.toString(convertListToArray(localTimestampVector)));
	}
	
	/**
	 * Helper method to convert int[] to ArrayList<Integer>
	 * @param ints
	 * @return ArrayList<Integer> where ArrayList(x) = ints[x].
	 */
	private ArrayList<Integer> convertArrayToList(int[] ints){
		ArrayList<Integer> intList = new ArrayList<Integer>(ints.length);
		
		for (int index = 0; index < ints.length; index++)
	    { 
			intList.add(ints[index]);
	    }
		return intList;
	}

	public static void main(String[] args) {
		int current_node = 2; 
		int total_nodes = 3;
		MembershipProtocol mp = new MembershipProtocol(current_node, total_nodes);
		
		int[] sampleForMerge = new int[total_nodes];
		int [] sampleForReceipt = new int[total_nodes];
		
		for(int i = 0; i< total_nodes; i++){
			sampleForMerge[i] = 1; 
			sampleForReceipt[i] = 0;
		}
		sampleForMerge[current_node] = 2;
		
		System.out.println("MemebershipProtocol:Test sampleForMerge "+Arrays.toString(sampleForMerge));		
		System.out.println("MemebershipProtocol:Test sampleForReceipt "+Arrays.toString(sampleForReceipt));

		
		mp.mergeVector(sampleForMerge);
		sampleForReceipt = mp.incrementAndGetVector();
		System.out.println("MemebershipProtocol:main() "+Arrays.toString(sampleForReceipt));
		
		mp.shutdown(null);
		mp.shutdown(0);
		mp.shutdown(1);
	}
}
