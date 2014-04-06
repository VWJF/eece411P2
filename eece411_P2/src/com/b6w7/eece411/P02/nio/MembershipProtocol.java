package com.b6w7.eece411.P02.nio;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
	public final int current_node;
	private ArrayList<Integer> localTimestampVector; //TODO: changed from int[] to Integer[]
	private Map<InetSocketAddress, Long> timeTCPTimeout = new HashMap<InetSocketAddress, Long>((int)(this.total_nodes / 0.7));
	/** Fraction in the range of (0, 1) used in multiplication with the current running average */
	public static double TIME_ALPHA = 0.9;
	/** The maximum timeout on a read operation used as default value and when a node becomes unresponsive */
	private final long timeMaxTimeout;  
	/** The minimum timeout on a read operation to prevent timeout from becoming too small */
	private final long timeMinTimeout;  
	
	private static boolean IS_DEBUG = true; //true: System.out enabled, false: disabled

	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);


	public MembershipProtocol(int current_node, int total_nodes, long timeMaxTimeout, long timeMinTimeout) {
		this.current_node = current_node;
		this.total_nodes = total_nodes;
		this.localTimestampVector = new ArrayList<Integer>(this.total_nodes);
		this.timeMaxTimeout = timeMaxTimeout;
		this.timeMinTimeout = timeMinTimeout;
		
		for (int i=0; i<this.total_nodes; i++) {
			localTimestampVector.add(1);
		}
	}
	
	/**
	 * Update the local timestamp vector based on the received vector timestamp 
	 * @param receivedVector
	 */
	public void mergeVector(int[] receivedVector){
		//behavior on receiving a vectorTimestamp at each node 
		
		if(receivedVector == null)
			return;
		
		ArrayList<Integer> updateTimestampVector = new ArrayList<Integer>(this.total_nodes);
		
		// TODO : We are accessing localTimestampVector from both threads, so synchronize
//		synchronized (localTimestampVector) {
			log.debug(" === mergeVector() (localTimestampVector.length=={}) (current_node=={})", localTimestampVector.size(), current_node);
			
			int local = localTimestampVector.get(current_node);
			log.debug(" === mergeVector() (localIndex={}) received vect: {}", local, Arrays.toString(receivedVector));
			log.debug(" === mergeVector() (localIndex={}) local vect   : {}", local, Arrays.toString(convertListToArray(localTimestampVector)));

			
			int i, localView, remoteView;
			int update;
			if (IS_DEBUG) 
				if (localTimestampVector.size() != receivedVector.length) 
					System.out.println(" ### MembershipProtocol::mergeVector() receivedVector.length==" + receivedVector.length);

			for(i = 0; i < localTimestampVector.size(); i++){

				localView = localTimestampVector.get(i).intValue();
				remoteView = receivedVector[i];
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

				updateTimestampVector.add(new Integer(update));
			}
			updateTimestampVector.set(current_node, local);

			localTimestampVector = updateTimestampVector;
			localTimestampVector.trimToSize();

			log.debug(" === mergeVector() (localIndex={}) after merging: {}", local, localTimestampVector);
	}

	/**
	 * Preparing the Vector Timestamp to be sent to a remote node.
	 * @return
	 */
	public int[] incrementAndGetVector(){
		ArrayList<Integer> retInteger;
		int update;

		update = localTimestampVector.get(current_node).intValue();
		if(update > 0)
			update++;
		
		localTimestampVector.set(current_node, update);
		retInteger = new ArrayList<Integer>(localTimestampVector);
		localTimestampVector.trimToSize();
		log.trace(" === incrementAndGetVector() after update: {}", retInteger);

		retInteger.trimToSize();
		int[] backingArray = convertListToArray(retInteger);
		log.trace(" === incrementAndGetVector() backingArray: {}", Arrays.toString(backingArray));

		return backingArray;
	}

	/**
	 * Return a random index in the range of localtimestampVector that does not match the current_node
	 * @return
	 */
	public Integer getRandomIndex(){
		//To obtain a unique random number to use as indices for localtimestampvector.
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
				test = localTimestampVector.get(randomIndex).intValue();
				log.debug(" === getRandomIndex() [localTimeStampVector[{}]=>{}]", randomIndex, test);
				if (randomIndex == current_node) {
					log.debug(" === getRandomIndex() skipping self [current_node=>{}]", randomIndex);
					continue;
				}
				if(test > 0){
					validRandomFound = true;
					break;
				}
			}while(  (--i) != 0  );
		}catch(NullPointerException npe){
			//A valid entry does not exist.
			//npe.printStackTrace();
			log.error(" === getRandomIndex() NullPointerException: {}", npe.getLocalizedMessage());
			randomIndex = current_node;
		}catch(IndexOutOfBoundsException iob){
			//A valid entry does not exist.
			//iob.printStackTrace();
			log.error(" === getRandomIndex() IndexOutBndExcept: {}", iob.getLocalizedMessage());
			randomIndex = current_node;
		}
		log.debug(" === getRandomIndex() post: {}", randomIndex);

		if(validRandomFound)
			return new Integer(randomIndex);
		 
		return null;
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
		return localTimestampVector.get(nodeIndex).intValue();
	}
	
	public void shutdown(Integer updateIndex){
		//TODO:
		
		if (localTimestampVector == null) 
			log.error(" ### localTimestampVector is null");

		if(updateIndex == null){
			localTimestampVector.set(current_node, -Math.abs(localTimestampVector.get(current_node)));
		}
		else{
			if (updateIndex.intValue() != current_node)
				localTimestampVector.set(updateIndex.intValue(), -Math.abs(localTimestampVector.get(updateIndex.intValue())));
			else {
				log.trace(" *** MembershipProtocol::shutdown() shutdown self attempted with index {} instead of null", updateIndex.intValue());
				return;
			}
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
		MembershipProtocol mp = new MembershipProtocol(current_node, total_nodes, 750, 350);
		
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

	/**
	 * Retrieve the timeout for this {@code owner}.  If {@code owner} does not exist then instantiate entry 
	 * for {@code owner} with the default value {@link #timeMaxTimeout}.
	 * @param owner the remote node
	 * @return the timeout in ms for this owner
	 */
	public long getTimeout(InetSocketAddress owner) {
		if (! timeTCPTimeout.containsKey(owner)) {
			// we have not seen this owner before, so instantiate entry and assign timeMaxTimeout
			log.trace("     Membership::getTimeout() ADDING [timeTCPTimeout+={}] {}", owner, this);
			timeTCPTimeout.put(new InetSocketAddress(owner.getAddress(), owner.getPort()), Long.valueOf(timeMaxTimeout));
			assert(timeTCPTimeout.containsKey(owner));
		}	
		
		return timeTCPTimeout.get(owner).longValue();
	}
	
	/**
	 * Update the running average read timeout for this {@code owner}.  The timeout is calculated as
	 * twice of the average time to complete a read operation.  The running average feedback is
	 * {@link #TIME_ALPHA}.  If {@code timeLastCompletion} is zero, then reset timeout to 
	 * {@link #timeMaxTimeout}.  Timeout cannot go below {@link #timeMinTimeout}
	 * @param timeLastCompletion the time in ms to complete a read operation
	 * @param owner the remote node being read from
	 */
	public void updateTimeout(long timeLastCompletion, InetSocketAddress owner) {
		long timeNewTimeout;
		
		if (! timeTCPTimeout.containsKey(owner)) {
			log.error(" ### Membership::updateTimeout() " + owner + " does not exist");
			throw new IllegalStateException(" ### Membership::updateTimeout() " + owner + " does not exist");
		}
		
		log.trace("     Membership::updateTimeout() BEFORE [{}] [timeLastCompletion=>{}] [timeout=>{}]", owner, timeLastCompletion, timeTCPTimeout.get(owner).longValue());

		// update read timeout for this owner
		if (timeLastCompletion < 0) {
			timeNewTimeout = timeMaxTimeout;

		} else {
			// if non-zero time from last iteration, 
			timeNewTimeout = (long)(timeTCPTimeout.get(owner).longValue() * TIME_ALPHA  + 2 * timeLastCompletion * (1 - TIME_ALPHA));
			if (timeNewTimeout < timeMinTimeout)
				timeNewTimeout = timeMinTimeout;
		}
		
		// cap timeout at timeMaxTimeout
		if (timeNewTimeout > timeMaxTimeout)
			timeNewTimeout = timeMaxTimeout;

		timeTCPTimeout.put(owner, Long.valueOf(timeNewTimeout));

		log.trace("     Membership::updateTimeout() AFTER [{}] [timeout=>{}]", owner, timeNewTimeout);

		// This is here to see the timeouts for all the other nodes
		if (log.isTraceEnabled()) {
			StringBuilder s = new StringBuilder();
			for (Map.Entry<InetSocketAddress, Long> set : timeTCPTimeout.entrySet()) {
				InetSocketAddress addr = set.getKey();
				Long timeout = set.getValue().longValue();
				s.append("[" + addr.getHostName().substring(0, 6) + ":" + addr.getPort() + ":" + timeout + "]");
			}
			log.error(s.toString());
		}
	}
}
