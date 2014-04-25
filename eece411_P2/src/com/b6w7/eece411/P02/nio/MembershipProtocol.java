package com.b6w7.eece411.P02.nio;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;

public class MembershipProtocol extends Observable{

	/** index of local node(service) participating in the MemebershipProtocol. */
	public final int current_node;
	
	/** number of total nodes participating in the MemebershipProtocol. */
	private final int total_nodes;
	/** Internal representation of timestamp vector. */
	private ArrayList<Integer> localTimestampVector;

	/** Internal representation of {@code localTimestampVector} to be used to detect changes to the membership protocol. */
	private ArrayList<Integer> oldTimestampVector;

	/** TODO: Write description of data structure purpose. */
	private Map<InetSocketAddress, Long> timeTCPTimeout = new HashMap<InetSocketAddress, Long>((int)(this.total_nodes / 0.7));
	/** Fraction in the range of (0, 1) used in multiplication with the current running average */
	public static double TIME_ALPHA = 0.9;
	/** The maximum timeout on a read operation used as default value and when a node becomes unresponsive */
	private final long timeMaxTimeout;  
	/** The minimum timeout on a read operation to prevent timeout from becoming too small */
	private final long timeMinTimeout;

	/** Reference to the Map that this MembershipProtocol is working with. */
//	private ConsistentHashing<ByteArrayWrapper, byte[]> map;  
	
	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);


	public MembershipProtocol(int current_node, int total_nodes, long timeMaxTimeout, long timeMinTimeout) {
		this.current_node = current_node;
		this.total_nodes = total_nodes;
		this.localTimestampVector = new ArrayList<Integer>(this.total_nodes);
		this.oldTimestampVector = new ArrayList<Integer>(this.total_nodes);
		this.timeMaxTimeout = timeMaxTimeout;
		this.timeMinTimeout = timeMinTimeout;
		
		for (int i=0; i<this.total_nodes; i++) {
			localTimestampVector.add(-1);
			oldTimestampVector.add(-1);
		}
		
		localTimestampVector.set(current_node, 2);
		oldTimestampVector.set(current_node, 2);
	}
	
	
//	public void setMap(ConsistentHashing<ByteArrayWrapper, byte[]> ch){
//		this.map = ch;
//	}
	
	/**
	 * Update the local timestamp vector based on the received vector timestamp 
	 * @param int[] receivedVector
	 */
	public void mergeVector(int[] receivedVector){
		//behavior on receiving a vectorTimestamp at each node 
		
		if(receivedVector == null)
			return;
		
		ArrayList<Integer> updateTimestampVector = new ArrayList<Integer>(this.total_nodes);
		
			log.debug(" === mergeVector() (localTimestampVector.length=={}) (current_node=={})", localTimestampVector.size(), current_node);
			
			int local = localTimestampVector.get(current_node);
			log.debug(" === mergeVector() (localIndex={}) received vect: {}", local, Arrays.toString(receivedVector));
			log.debug(" === mergeVector() (localIndex={}) local vect   : {}", local, localTimestampVector );

			
			int i, localView, remoteView;
			int update;
			if (localTimestampVector.size() != receivedVector.length) 
				log.warn(" ### MembershipProtocol::mergeVector() (localTimestampVector.size(), receivedVector.length) = ({},{})", localTimestampVector.size(), receivedVector.length);

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

			log.debug(" === mergeVector() (localIndex={}) after merging: {}", local, localTimestampVector);
			
			//notifyViewers("gossip");
	}

	/**
	 * Preparing the Vector Timestamp to be sent to a remote node.
	 * @return int[] of the node timestamps after incrementing the local node's timestamp
	 */
	public int[] incrementAndGetVector(){
		ArrayList<Integer> retInteger;
		int update;

		update = localTimestampVector.get(current_node).intValue();
		if(update > 0)
			update++;
		
		localTimestampVector.set(current_node, update);
		//FIXME: Potentially retInteger not needed.
		retInteger = new ArrayList<Integer>(localTimestampVector);
		
		log.debug(" === incrementAndGetVector() {}", retInteger);

		int[] backingArray = convertListToArray(retInteger);
		log.trace(" === incrementAndGetVector() backingArray: {}", retInteger);

		if(localTimestampVector.size() != retInteger.size())
			log.warn(" ### MembershipProtocol::incrementAndGetVector() (localTimestampVector.size(), retrunedVector.length) = ({},{})", localTimestampVector.size(), retInteger.size());
		
		return backingArray;
	}

	/**
	 * Return a random index in the range of localtimestampVector that does not match 
	 * the current_node and is online (localtimestampVector[return_index] > 0)
	 * @return Integer index of a random online node.
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
	 * @param retInteger: ArrayList<Integer>
	 * @return int[] of size total_nodes such that int[x] = ArrayList<Integer>.get(x)
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
	 * @params nodeIndex: Index of timestamp entry to return. {@link #total_nodes} 0 > nodeIndex >= 0 
	 * @return int timestamp entry of nodeIndex
	 */
	public int getTimestamp(int nodeIndex){
		return localTimestampVector.get(nodeIndex).intValue();
	}
	
	/**
	 * Disable this node's timestamp.  If the timestamp is negative then nothing changes. 
	 * If the timestamp is positive, then it is negated.
	 * @param updateIndex of the node
	 * @return {@code true} if an online node has been set offline, {@code false} otherwise  
	 */
	public boolean shutdown(Integer updateIndex){
		boolean isShutdown = false;
		
		if (localTimestampVector == null) 
			log.error(" ### localTimestampVector is null");

		if(updateIndex == null){
			int oldTime = localTimestampVector.get(current_node);
			if( oldTime > 0){
				int newTime = -1 * oldTime;
				localTimestampVector.set(current_node, newTime);
				isShutdown = true;	
			}
		}
		else{
			if (updateIndex.intValue() != current_node) {
				int time = localTimestampVector.get(updateIndex.intValue());
				if (time > 0) {
					// we only have work if this node is online
					int newTime = -1 * time;
					localTimestampVector.set(updateIndex.intValue(), newTime);
					isShutdown = true;
					log.info("     Shutting down an unresponsive node [time=>[{}]->[{}]] [index=>{}] [vect:->{}]", time, newTime, updateIndex, localTimestampVector);
					
//					notifyViewers("shutdown");
				}
			}
			else {
				isShutdown = false;
				log.trace(" *** MembershipProtocol::shutdown() shutdown self attempted with index {} instead of null", updateIndex.intValue());
				return isShutdown;
			}
		}
		int shutdownIndex;
		if(updateIndex == null)
			shutdownIndex = -1;
		else
			shutdownIndex = updateIndex.intValue();
		log.debug(" === shutdownindex {}, {} {}", shutdownIndex, isShutdown, localTimestampVector );
		
		return isShutdown;
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

		log.warn("     Membership::updateTimeout() AFTER [{}] [timeout=>{}]", owner, timeNewTimeout);

		// This is here to see the timeouts for all the other nodes
		if (log.isTraceEnabled()) {
			StringBuilder s = new StringBuilder();
			for (Map.Entry<InetSocketAddress, Long> set : timeTCPTimeout.entrySet()) {
				InetSocketAddress addr = set.getKey();
				Long timeout = set.getValue().longValue();
				s.append("[" + addr.getHostName().substring(0, 6) + ":" + addr.getPort() + ":" + timeout + "]");
			}
			log.trace(s.toString());
		}
	}


	/**
	 * Enable the index of this node.  An online node is unaffected.  An offline node is set to positive of itself.
	 * @param index of node
	 * @return {@code true} if an offline node has been brought online, {@code false} otherwise  
	 */
	public boolean enable(int index) {
		boolean isEnabled = false;
		
		if (localTimestampVector == null) 
			log.error(" ### MembershipProtocol::enable() localTimestampVector is null");

		if (index != current_node) {
			int time = localTimestampVector.get(index);
			if (time < 0) {
				// only have work to do if the timestamp is negative
				int newTime = Math.abs(time);
				localTimestampVector.set(index, newTime);
				isEnabled = true;

				log.info("     Enabling a responsive node [time=>[{}]->[{}]] [index=>{}] [vect->{}]", time, newTime, index, localTimestampVector);
				
//				notifyViewers("enable");			
			}
		} else { 
			isEnabled = false;
			log.warn(" *** MembershipProtocol::enable() enable self attempted with index {}", index);
		}
		
		return isEnabled;
	}
	
	/**
	 * Detects differences in the {@code oldTimestampVector} with up-to-date {@code localTimestampVector}.
	 * Detects differences among all entries except the one representing current_node.
	 * @return true if differences were found, false if not.
	 */
	private boolean hasTimestampChanged(){
		ArrayList<Integer> oldVector = new ArrayList<Integer>(oldTimestampVector);
		ArrayList<Integer> newLocal = new ArrayList<Integer>(localTimestampVector);

		log.debug("Membership Detecting changes: oldtimestamp: {} ", oldTimestampVector);
		log.debug("Membership Detecting changes: localtimestamp: {} ", localTimestampVector);


		boolean differencesFound = false;

		//Detect all changes except those to the current_node;
		int oldVal = oldVector.remove(current_node);
		int current_val = newLocal.remove(current_node);

		//boolean differencesFound = oldVector.removeAll(newLocal);
				
		int i = 0;
		while( i < oldVector.size() ){
			if( oldVector.get(i) != newLocal.get(i))
				differencesFound = true;
			i++;
		}
		
		oldVector.add(current_node, current_val);
		newLocal.add(current_node, current_val);
		
		if(differencesFound){
			oldTimestampVector = newLocal;
		}
		
		log.debug("MemebershipProtocol Determined changes to report to Observers. {} ", differencesFound);
		
		return differencesFound;
	}
	
//	/**
//	 * Method to notify Observers of this class of changes.
//	 * @param sourcestate a reply string for the Observer to interpret.
//	 */
//	public void notifyViewers(String sourcestate){
//		if( hasTimestampChanged() ){
//			log.trace("MemebershipProtocol is notifying viewers.");
//			// We can choose to notify observers with other args if we chose to.
//			// e.g. notifyViewers() from enable can respond with args="enable", or args=localTimestamp.
//			//setChanged();			
//			//notifyObservers(localTimestampVector);
//			//notifyObservers(sourcestate);
//			map.update(sourcestate);
//		}
//	}
	
	/**
	 * Helper Methods that Simulates increasing timestamps, shutdown.
	 * Used to test MemebershipProtocol & ConsistentHashing.
	 * Change scope to private when deploying
	 * Change scope to public when testing.
	 */
	
	/**
	 * Simulated increasing timestamp entries. 
	 * Does not differentiate between positive/negative entry.
	 * 
	 * Change scope to private when deploying
	 * Change scope to public when testing.
	 */
	public ArrayList<Integer> incrementAnEntry(){
		Random rand = new Random();
		int index = rand.nextInt(total_nodes);
		Integer oldTime = localTimestampVector.get(index);
		oldTime++;
		localTimestampVector.set(index, oldTime);
		
		//notifyViewers("gossip");
		
		return localTimestampVector;
	}
	/**
	 * Simulates receiving shutdown command of any random node.
	 * Change scope to private when deploying
	 * Change scope to public when testing.
	 * @return index of the node shutdown, null if the node shutdown is {@link current_node}
	 */
	public Integer shutdownAnyEntry(){
		Random rand = new Random();
		Integer index = rand.nextInt(total_nodes);
		if(index == current_node){
			index = null;
		}
		
		boolean success = shutdown(index);
		//System.out.println("Message from Memebership.shutdown(Integer): "+success);
		return index;
	}
	
	/**
	 * Simulates receiving enable command for any random.
	 * Change scope to private when deploying
	 * Change scope to public when testing.
	 * @return index of the node shutdown, null if the node shutdown is {@link current_node}
	 */
	public Integer enableAnyEntry(){
		Random rand = new Random();
		Integer index = rand.nextInt(total_nodes);
		if(index == current_node){
			index = null;
		}
		
		boolean success = enable(index);
		//System.out.println("Message from Memebership.enable(int): "+success);
		return index;
	}
}
