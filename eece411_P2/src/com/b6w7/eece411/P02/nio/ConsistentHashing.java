package com.b6w7.eece411.P02.nio;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Observable;
import java.util.Observer;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;


public class ConsistentHashing<K, V> implements Map<ByteArrayWrapper, byte[]>,
									Observer{

	public static boolean IS_DEBUG = true; //true: System.out enabled, false: disabled
	public static boolean IS_VERBOSE = true; //true: System.out enabled, false: disabled

	private static int num_replicas = NodeCommands.REPLICATION_FACTOR; //Can be initialized through the constructor

	private static Logger log = LoggerFactory.getLogger(ServiceReactor.class);

	/**The structure used to maintain the view of the pairs (key,value) & participating nodes.
	 * Should be initialized to MAX_MEMORY*(1/load_factor), to limit the number of keys & avoid resizing.
	 * */
	private HashMap<ByteArrayWrapper, byte[]> circle;
	
	/**The structure used to maintain the view of the participating nodes. */
	private final TreeMap<ByteArrayWrapper, byte[]> mapOfNodes; // = new TreeMap<ByteArrayWrapper, byte[]>();

	/** The structure used to maintain the record of the natural ordering within the map of nodes.*/
	private final List<ByteArrayWrapper> listOfNodes;
	
	/*** Maintains the current list of nodes that host replicas for primary key-values that belong to localhost. */
	private ArrayList<InetSocketAddress> localReplicaList = new ArrayList<InetSocketAddress>();

	/*** Maintains the last known online predecessor to localhost. */
	private ByteArrayWrapper onlinePredecessor;

	/** The structure used to maintain in sorted order the Keys that are present in the local Key-Value store. */
	private TreeMap<ByteArrayWrapper, byte[]> orderedKeys;
	
	/** Variable used to maintain identity of the key for the localhost(local ServiceReactor). */
	private ByteArrayWrapper localNode;

	/** Used to store the Keys that are expected to be transfered/removed. In order to avoid repeated request to TreeMap. */
	private Set<ByteArrayWrapper> transferSet = null;

	/** The membership protocol used by this Consistent Hashing to determine and set nodes online/offline. */
	private MembershipProtocol membership;
	private MessageDigest md;

	public ConsistentHashing(String[] nodes) throws NoSuchAlgorithmException, UnsupportedEncodingException {


		this.circle = new HashMap<ByteArrayWrapper, byte[]>((int)(40000*1.2)); //Initialized to large capacity to avoid excessive resizing.
		this.mapOfNodes = new TreeMap<ByteArrayWrapper, byte[]>();
		this.md = MessageDigest.getInstance("SHA-1");
		
		int i = 0;
		for (String node : nodes){	
			log.debug("{}.{}", i, node);
			byte[] digest = md.digest(node.getBytes());
			
			log.trace("     ConsistentHashing() digest: {}", NodeCommands.byteArrayAsString(digest));
			//ByteArrayWrapper key = hashKey(digest);
			ByteArrayWrapper key = hashKey(node);
			log.trace("     ConsistentHashing() hashKey: {}", key);

			byte[] fromMap = mapOfNodes.put(key, node.getBytes());
			circle.put(key, node.getBytes());			
			String err_msg = (null == getNode(hashKey(node))) ? " *** ConsistentHashing() Fail to get" : "     ConsistentHashing() Success returned get(): "+
					NodeCommands.byteArrayAsString(getNode(hashKey(node)));
			
			log.trace(err_msg);
			log.debug("     ConsistentHashing() Map Of Nodes Size: {}", mapOfNodes.size());
			log.debug("     ConsistentHashing() {}.{} {}", i, hashKey(node), new String(getNode(key)));
			if(fromMap != null){
				StringBuilder conflict = new StringBuilder(new String(fromMap));
				log.error(" ### ConsistentHashing() Encountered collision entry for mapOfNodes: original: {} new: {} ", node, conflict.toString());
			}
			//assert(fromMap != null);
			i++;
		}
		
		log.info("     ConsistentHashing() Map of Nodes Size at Constructor: {}", mapOfNodes.size());
		
		listOfNodes = new ArrayList<ByteArrayWrapper>(mapOfNodes.keySet());
		Collections.sort(listOfNodes);

		log.info("     ConsistentHashing() Ordered List of Nodes Size at Constructor: {}", listOfNodes.size());
		assert (listOfNodes.size() == mapOfNodes.size());

		// circle has been initialized with the pairs of (Node, hostname).
		// Create a new view containing only the existing nodes.
		orderedKeys = new TreeMap<ByteArrayWrapper, byte[]>(circle);//Initialized to large capacity to avoid excessive resizing.
		//orderedKeys.addAll(circle.keySet());
	}
	
	public void setMembership(MembershipProtocol membership) {
		this.membership = membership;
		initialSetup();
	}
	/**
	 * TODO: update {@link this.localReplicaList} according to right implementation.
	 * Setup method that will initialize {@link localNode}, {@link onlinePredecessor} 
	 * and {@link localReplicaList} with the node that was created using {@code key}
	 * @param key key that was used to create the Key in <Key,Value> pair in the map of nodes.
	 * @throws IllegalArgumentException unchecked if the initialization was unsuccessful. 
	 * 			i.e. @param key could not be found.
	 */
	public void setLocalNode(String key) {
		localNode = getOwner(hashKey(key));
		if(localNode == null){
			log.error("Failed to setup ConsistentHashing. Local node {} could not be found in the map of nodes.", key);
			throw new IllegalArgumentException("Failed to setup ConsistentHashing. Local node " +key+ " could not be found in the map of nodes.");
		}

		this.onlinePredecessor = getPreviousNodeTo(localNode);
		//this.localReplicaList = getReplicaList(); //this.localReplicaList = getLocalReplicaList();
		if(membership != null){
			initialSetup();
		}
		log.debug("setLocalNode(): {} {}",key, localNode);		
	}

	/**
	 * Intialize onlinePredecesor and localReplicaList after setLocalNode() and setMembership(). 
	 * @return {@code true}  if onlinePredecesor and localReplicaList have been initialized. 
	 * 		   {@code false} if there were already initialized.
	 * 
	 */
	public boolean initialSetup() {
		boolean initialSetup = false;
		if(membership == null)
			initialSetup = false;
		
		if( onlinePredecessor == null){
			this.onlinePredecessor = getPreviousResponsible(localNode);
			initialSetup = initialSetup ^ true;
		}
		// TODO: set this.localReplicaList. Being developed.
		if( localReplicaList == null){
			//this.localReplicaList = getReplicaList(); //this.localReplicaList = getLocalReplicaList();
			initialSetup = initialSetup && true;
		}
		return initialSetup;
		
	}
	
	/**
	 * Accessor
	 * @return localnode
	 */
	public ByteArrayWrapper getLocalNode() {
		return localNode;
	}
	
	private byte[] addNode(ByteArrayWrapper key, byte[] value) {
				return mapOfNodes.put(key, value);
	}
	
	public byte[] getNode(ByteArrayWrapper key) {
		if (mapOfNodes.isEmpty()) {
			return null;
		}
		return mapOfNodes.get(key);
	}
	
	/**
	 * Given a host-name, obtain its position (the natural ordering) within the map of Nodes
	 * Assumption, the given host-name is present in the list of nodes participating.
	 * @param node	String of the hostname to find.
	 * @return index of the hostname
	 */
	public int getNodePosition(String node){
		ByteArrayWrapper key = hashKey(node);
		log.debug("     ConsistentHashing::getNodePosition()  hashKey: {}", key);
		int ret = listOfNodes.indexOf(key);
		if (-1 == ret)
			log.error(" ### ConsistentHashing::getNodePosition() index not found for node {}", node);
		return ret;
	}
	
	/**
	 * @return total number of nodes in the Key-value store. Does not check for online/offline
	 */
	public int getSizeAllNodes(){
		return listOfNodes.size();
	}

	/**
	 * Adds the given (Key,Value) entry to the Key-Value Store Map & Ordered list of Keys.
	 * thread that uses the Map should impose additional restrictions.
	 * 	if(circle.size() == Command.MAX_MEMORY && circle.containsKey(key)){
	 */
	@Override
	public byte[] put(ByteArrayWrapper key, byte[] value) {
		// Additional Checking unnecessary since the thread that
		// uses the Map should impose additional restrictions.
		//if(circle.size() == Command.MAX_MEMORY && circle.containsKey(key)){
		orderedKeys.put(key, value);//add( key );
		return circle.put(key, value);
	}

	private byte[] remove(ByteArrayWrapper key) {
		
		orderedKeys.remove(key);
		return circle.remove(key);
	}

	private byte[] get(ByteArrayWrapper key) {
		if (circle.isEmpty()) {
			return null;
		}
		return circle.get(key);
	}
	
	/**
	 * Obtains the InetSocketAddress of a random node that MemebershipProtocol views online.
	 * Returns null if one cannot be found.
	 * @return InetSocketAddress of the random node.
	 */
	public InetSocketAddress getRandomOnlineNode() {
		log.trace("     ConsistentHashing.getRandomOnlineNode()");
		if (mapOfNodes.isEmpty()) {
			log.warn(" ### ConsistentHashing.getRandomOnlineNode() Map of Nodes Empty.");
			return null;
		}
		
		Integer randomIndex = membership.getRandomIndex();
		if(randomIndex == null) {
			log.warn("ConsistentHashing.getRandomOnlineNode() No online nodes.");
			return null;
		}
		
		log.trace("     ConsistentHashing.getRandomOnlineNode() Index: {}", randomIndex);
		ByteArrayWrapper node = listOfNodes.get(randomIndex.intValue());

		// we found a random element
		String nextHost = new String(mapOfNodes.get(node));
		log.trace("     ConsistentHashing.getRandomOnlineNode() CHOSEN    {}", nextHost);

		String addr[] = nextHost.split(":");
		return new InetSocketAddress(addr[0], Integer.valueOf(addr[1]));
	}
	
	/**
	 * Obtains the node(IP address) that is responsible for the requested key in the Key-Value Store circle 
	 * The keys(arc) that a node is considered responsible for are (previousNode:exclusive, currentNode:inclusive].
	 * Helper method getNodeResponsible() deems if a node is online/offline.
	 * @param requestedKey
	 * @return The node responsible for {@code requestedKey} as a InetSocketAddress.
	 */
	public InetSocketAddress getSocketNodeResponsible(ByteArrayWrapper requestedKey) {
		ByteArrayWrapper nextKey = getNodeResponsible(requestedKey);
		
		String nextHost = new String(mapOfNodes.get(nextKey));
		String nextOfValue = "(key,value) does not exist in circle";
		if(circle.get(requestedKey)!= null)
			nextOfValue = new String(circle.get(requestedKey));
		log.trace("NextOf: {}[value->{}]\nis the target TargetHost: {} [value->{}]", requestedKey.toString(), nextOfValue, nextKey, nextHost);

		String addr[] = nextHost.split(":");

		log.trace("Finding InetSocketAddress.");
		return new InetSocketAddress(addr[0], Integer.valueOf(addr[1]));
	}

	/**
	 * return a list of nodes that host replicas for primary key-values of localhost
	 * @return List<InetSocketAddress> TODO
	 */
	public List<InetSocketAddress> getLocalReplicaList() {

		ArrayList<InetSocketAddress> newReplicas = updateLocalReplicaList();		
				
		// TODO
		// for now just pass through, but we might want to be saving it here, and just before
		// saving, comparing it to the old copy.  If there is a difference, then a transfer of 
		// keys is in order.
		
		hasReplicaChanged();
		
		return new ArrayList<InetSocketAddress>(newReplicas);
	}

	private ArrayList<InetSocketAddress> updateLocalReplicaList() {
		ArrayList<InetSocketAddress> replicas = new ArrayList<InetSocketAddress>();
		ByteArrayWrapper nextNode;
		ByteArrayWrapper startingNode = getLocalNode();
		
		InetSocketAddress startingInet = getSocketNodeResponsible(startingNode);
		int numReplicasLeftToAdd = num_replicas;
		InetSocketAddress nextInet = null;
		boolean hasLooped = false;
		
		// false excludes self from the list
		SortedMap<ByteArrayWrapper, byte[]> sortedMap = mapOfNodes.tailMap(startingNode, false);
		
		// If we have an empty list, then we are at end of circle, 
		// so start at beginning of circle
		if (sortedMap.size() == 0)  {
			sortedMap = mapOfNodes;
			hasLooped = true;
		}
		
		Iterator<ByteArrayWrapper> iter = sortedMap.keySet().iterator();
		nextNode = iter.next();
		nextInet = getSocketNodeResponsible(nextNode);
		
		// we assume that localnode is in the mapOfNodes,
		// so always at least one element in mapOfNodes
		
//		FIND_REPLICAS: while (!nextInet.equals(startingInet) && numReplicasLeftToAdd > 0) {
		FIND_REPLICAS: while (Handler.compareSocket(nextInet, startingInet) != 0 && numReplicasLeftToAdd > 0) {
			
//			if (membership.getTimeout(nextInet) > 0) {
//				// This node is online, so we can use it
//				// This node is online, so we add to replicaList
//				replicas.add(nextInet);
//				numReplicasLeftToAdd --;
//			}

			// No need to check for online because getSocketNodeResponsible does that already
			replicas.add(nextInet);
			//this.localReplicaListMap.add(nextInet);
			numReplicasLeftToAdd --;
			
			// check to see if we reached end of circle, if so, start
			// at beginning
			if (!iter.hasNext()) {
				if (!hasLooped) {
					hasLooped = true;
					iter = mapOfNodes.keySet().iterator();
				} else {
					break FIND_REPLICAS;
				}
			}

			nextNode = iter.next();
			nextInet = getSocketNodeResponsible(nextNode);
		}
		
		return replicas;
	}
	
	/**
	 * return a list of offline nodes
	 * @return List<InetSocketAddress> where entries are InetSocketAddress of all nodes deemed offline by membership protocol 
	 */
	public List<InetSocketAddress> getOfflineList() {
		List<InetSocketAddress> offlineList = new LinkedList<InetSocketAddress>();
		ByteArrayWrapper node;
		
		for (int i = 0; i < listOfNodes.size(); i++) {
			if (membership.getTimestamp(i) < 0) {
				node = listOfNodes.get(i);

				String nodeString = new String(mapOfNodes.get(node));
				String addr[] = nodeString.split(":");
				
				offlineList.add(new InetSocketAddress(addr[0], Integer.valueOf(addr[1])));
			}
		}
		return offlineList;
	}

	/**
	 * Given a requestedKey, replies with a List of InetSockAddress for the primary node/owner + num_replicas (successors) of the requestedKey
	 * @param requestedKey
	 * @param removeSelf
	 * @return List of the online replicas of requestedKey. 
	 * 		   If (removeSelf == true), then excludes {@link localNode} from returned list.
	 * Check "FIXME" near end of method.
	 */
	public ArrayList<InetSocketAddress> getReplicaList(ByteArrayWrapper requestedKey, boolean removeSelf) {
		ArrayList<InetSocketAddress> replicas = new ArrayList<InetSocketAddress>(num_replicas+1);
		InetSocketAddress sockAddress = null;
		ByteArrayWrapper owner = getOwner(requestedKey);

		sockAddress = getSocketAddress(owner);
		
		StringBuilder logtraceString = null;
		if (log.isTraceEnabled()) {
			logtraceString = new StringBuilder();
			logtraceString.append("owner: "+owner.toString()+"[requestedKey->"+requestedKey+"]\nSocketAddress "+ sockAddress.toString()+"\n");
		}
		
		log.trace("All Replicas:\n[owner->{}][requestedKey->{}] [SocketAddress->{}]", owner.toString(), requestedKey, sockAddress.toString() );
		
		ByteArrayWrapper nextKey = owner;
		ByteArrayWrapper firstReplica = null; 

		// Iterate to find at least one node that is online and save that watermark with firstReplica
		// There is possibility that all nodes are offline causing an infinite loop for the case that
		// the local node has signaled shutdown and all other nodes are offline.
		// However, we can ignore this case, because the local node is shutting down anyways
		while (replicas.size() == 0) {
			int timestamp = membership.getTimestamp(listOfNodes.indexOf(nextKey)); 
			if ( timestamp < 0) {
				log.trace("     ConsistentHashing::getReplicaList() skipping offline node [{}] [timestamp=>{}]", sockAddress, timestamp);
				
			} else {
				firstReplica = nextKey;
				sockAddress = getSocketAddress(nextKey);
				replicas.add(sockAddress);

				if (log.isTraceEnabled())
					logtraceString.append("replica 0 : "+nextKey.toString() +" SocketAddress: " + sockAddress.toString()+"\n");
			}
			
			nextKey = getNextNodeTo(nextKey);
		}
		
		// ok, we now have one entry in replicas, now it is time to 
		// Iterate to find replicas
		for (int i = 0; i < num_replicas; i++) {
			int timestamp = membership.getTimestamp(listOfNodes.indexOf(nextKey)); 
			
			if ( timestamp < 0) {
				// if this node is offline, then skip this iteration
				// we are skipping this node, so undo counter
				i--;  
				if (log.isTraceEnabled())
					logtraceString.append("     ConsistentHashing::getReplicaList() skipping offline node ["+sockAddress+"] [timestamp=>"+timestamp+"]\n");
				
			} else {
				if (nextKey == owner) {
					// if we have gone full circle and reached the node responsible for this key, then stop iterating here
					if (log.isTraceEnabled())
						logtraceString.append("     ConsistentHashing::getReplicaList() back to owner ["+sockAddress+"] [timestamp=>"+timestamp+"]\n");
					break;
				}
				
				if (nextKey == firstReplica) {
					// if we have gone full circle and reached the first replica for this key, then stop iterating here
					if (log.isTraceEnabled())
						logtraceString.append("     ConsistentHashing::getReplicaList() back to first replica ["+sockAddress+"] [timestamp=>"+timestamp+"]\n");
					break;
				}

				sockAddress = getSocketAddress(nextKey);
				replicas.add(sockAddress);

				if (log.isTraceEnabled())
					logtraceString.append("replica "+i+" : "+nextKey.toString() +" SocketAddress: " + sockAddress.toString()+"\n");
			}

			nextKey = getNextNodeTo(nextKey);
		}
		
		if (log.isTraceEnabled()) {
			log.trace("{}",logtraceString.toString());
		}

		log.debug("Local key: {}", localNode);
		log.debug("Local key SocketAddress: {}", getSocketAddress(localNode));
		
		if(removeSelf) replicas.remove(getSocketAddress(localNode));
		
		//FIXME: Setting the member variable was used for testing purposes, 
		// unsure if it is still needed.
		//this.localReplicaList = replicas;
		
		return new ArrayList<InetSocketAddress>(replicas);
	}
	
	/**
	 * Helper method to obtain the InetSocketAddress for a key(that represents a node entry)
	 * @param node
	 * @return InetSocketAddress of {@code node} 
	 */
	private InetSocketAddress getSocketAddress(ByteArrayWrapper node){
		String addr[];
		try{
			addr = (new String(mapOfNodes.get(node))).split(":");
		}catch(NullPointerException e){
			log.warn("Searching for inexistent node in node map: "+node);
			throw new NoSuchElementException("Searching for inexistent node in node map: "+node);
		}
		
		log.trace("Finding InetSocketAddress.");
		return new InetSocketAddress(addr[0], Integer.valueOf(addr[1]));
	}

	/**
	 * Helper method that obtain the owner of requested key
	 * @param requestedKey:	node or data. ByteArrayWrapper whose owner to find.
	 * @return ByteArrayWrapper of the owner of {@code requestedKey}. {@code null} there aren't nodes.
	 */
	private ByteArrayWrapper getOwner(ByteArrayWrapper requestedKey){
		if (mapOfNodes.isEmpty()) {
			log.debug("Map Of Nodes Empty.");
			return null;
		}

		ByteArrayWrapper nextKey;
		SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(requestedKey);
		nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();
		
		return nextKey;
	}
	
	/** Helper method that obtains the subsequent node responsible/owner of a requested key with a valid timestamp
	 * @param requestedKey:	node or data whose successor to find.
	 * @return The node successor responsible for {@code requestedKey} which is deemed alive 
	 * by membership protocol. {@code null} if there aren't nodes alive.
	 */
	private ByteArrayWrapper getNodeResponsible(ByteArrayWrapper requestedKey) {
		if (mapOfNodes.isEmpty()) {
			log.debug("Map Of Nodes Empty.");
			return null;
		}

		ByteArrayWrapper nextKey;
		SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(requestedKey);
		nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();
		
		//FIXME: What is expected behavior ??
		// Scenario Shutdown (all nodes - 2). Use ConsistenHashing.main()
		// Are the results of Successor buggy??.
		// nextKey above may obtain the value of requestedKey,
		// instead use nextKey = getNextNodeTo(nextKey);
		nextKey = getNextNodeTo(nextKey);
		
		ByteArrayWrapper tempNextKey = nextKey;
		int time = this.membership.getTimestamp(listOfNodes.indexOf(nextKey));
		while(time < 0){
			nextKey = getNextNodeTo(nextKey);
			if( nextKey == tempNextKey ){
				byte[] get = circle.get(requestedKey);
				if(null != get) {
					String nextOfValue = new String(get);
					log.warn("#### Get(Remote)NodeResponsible has returned localnode{}[value->{}", nextKey.toString(), nextOfValue);
				}
				break;
			}		
			time = this.membership.getTimestamp(listOfNodes.indexOf(nextKey));
		}
		
		String requestedWithKey;
		if( mapOfNodes.containsKey(requestedKey) ){
			requestedWithKey = getSocketAddress(requestedKey).toString();
			log.trace("Requested for Node: {}", requestedWithKey);
		}
		
		String nextNodeSocket = getSocketAddress(nextKey).toString();
		String timestamp = Integer.toString(time);
		
		log.trace("Successor Node: {} with timestamp: {}", nextNodeSocket, timestamp);
		
		return nextKey;
	}
	
	/**
	 * Obtains the closest node(IP address) on the Key-Value Store circle 
	 * that is subsequent to the given key.
	 * "key" can be a key for a {@code node} or data.
	 * @param key	{@code key} whose subsequent to find.
	 * @return ByteArrayWrapper (key) for the next node of {@code key}.
	 */
	private ByteArrayWrapper getNextNodeTo(ByteArrayWrapper key) {
		if (mapOfNodes.isEmpty()) {
			log.debug("Map Of Nodes Empty.");
			return null;
		}
			ByteArrayWrapper nextKey;
			SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(key);
			nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();
			
			if (tailMap.containsKey(key) ==  true) {
				log.trace("** Key exists in tailMap. {}", NodeCommands.byteArrayAsString(nextKey.key) ) ;
				//if(tailMap.isEmpty() == false){
		//TODO: synchronized not needed. Check before removal.
				synchronized(mapOfNodes){
					Iterator<Entry<ByteArrayWrapper,byte[]>> is = tailMap.entrySet().iterator();
					//Skip the first entry since it is the element whose "next" we are trying to find.
					if(is.next() != null && is.hasNext())
						nextKey = is.next().getKey();
					else
						nextKey = mapOfNodes.firstKey();
				}
			}
			
			
			String nextHost = new String(mapOfNodes.get(nextKey));
			String nextOfValue = "(key,value) does not exist in circle";
			if(circle.get(key)!= null)
				nextOfValue = new String(circle.get(key));
			log.trace("NextOf: {}[value->{}]"+"is the target TargetHost: {} [value->{}]", key.toString(), nextOfValue, nextKey, nextHost);		

		return nextKey;
	}
	
	
	/**
	 * Method that extracts the keys, value pairs in range specified by {@code fromKey}, {@code toKey} 
	 * so that they may be transferred/removed to other nodes. 
	 * ({@code fromKey}, {@code toKey}] if fromKey < toKey (exclusive, inclusive] 
	 * ({@code toKey}, {@code fromKey}] if toKey < fromKey (exclusive, inclusive]
	 * @param fromKey limit to transfer.
	 * @param toKey: limit to transfer.
	 * @return Reference to the Set that is ready to be transferred.
	 */
	public Set<ByteArrayWrapper> transferKeys(ByteArrayWrapper fromKey, ByteArrayWrapper toKey){

		//InetSocketAddress destinationSocket = getSocketAddress(destinationNode);
		
		StringBuilder sb_extras = new StringBuilder();
		StringBuilder sb = new StringBuilder();
		sb.append("ConsistHash. transferKeys. ")
		.append("lowerKey: ").append(getSocketAddress(fromKey))
		.append(" upperKey: ").append(getSocketAddress(toKey));
		
		Set<ByteArrayWrapper> subSet = null, headSet = null;
		if( (fromKey.compareTo(toKey)) < 0	){
			subSet = orderedKeys.subMap(fromKey, false, toKey, true).keySet();
			sb.append(". lowerKey < upperKey.");
		}
		else{
			subSet = (orderedKeys.tailMap(fromKey, false).keySet());
			headSet = orderedKeys.headMap(toKey, true).keySet();
			sb.append(". upperKey < lowerKey : flip direction. ");
		}
		sb_extras.append("subSet.size: "+ subSet.size()+". ");

		Set<ByteArrayWrapper> fullSet = subSet;
		
		long totalKeysFound =  fullSet.size();
		try{
						
			if(headSet != null && headSet.isEmpty() == false){
				sb_extras.append("headSet.size: "+ headSet.size()+". ");
				// A new HashSet is needed because the NavigableSet returned 
				// by ordered key is backed by orderedKeys keySet.
				// The orderedKeys keySet does not support .allAll(Collection....) operation.
				fullSet = new HashSet<ByteArrayWrapper>(subSet.size() + headSet.size()); 
				fullSet.addAll(subSet);
				fullSet.addAll(headSet);
				totalKeysFound =  fullSet.size();
			}
		} catch(UnsupportedOperationException e){
			log.warn("{}",e.getLocalizedMessage());
		}

		sb.append("Total Keys found: ").append(totalKeysFound);

		sb.trimToSize();
		log.info(sb.toString());
		
		sb_extras.append("Total HashMap keyspace size: " + circle.size()+". ");
		sb_extras.append("Total TreeMap keyspace size: " + orderedKeys.size()+". ");
		sb_extras.append("fullSet.size: "+fullSet.size());	
		
		log.trace(sb_extras.toString());
		
		return fullSet;
	}
	
	/**
	 * Helper method used to instantiate adequate Handlers in order to transferKeys.
	 * Retrieves Keys from {@link tranferSet} populated by {@link transferKeys(fromKey, toKey)}.
	 * Retrieves values from HashMap circle.
	 * @param destinationNode 
	 * @param isSegmentRemove determines whether the key is to be put or removed on the {@code destinationNode}
	 * @return  {@code false} if the transferSet has not been populated. 
	 * 			{@code true}  after all Handlers have been instantiated to the given destination node
	 */
	private boolean createHandlerTransfers(InetSocketAddress destinationNode, boolean isSegmentRemove){
		
		boolean isComplete = false;
		

		if( 0 == Handler.compareSocket(destinationNode, getSocketAddress(localNode))){
			log.warn("ConsistHash. createHandlerTransfers() attempted to self. destination: {}", destinationNode);
			return false;
		}
		
		if(transferSet == null)	//transferKeys(Key:lowerLimit, Key:upperLimit) must be used to obtain the keys for transferring.
			return false;
		
		Iterator<ByteArrayWrapper> iter = transferSet.iterator(); //To get the set in sorted ascending order
		
		while( iter.hasNext() ){	
			ByteArrayWrapper sendKey = iter.next();
			byte[] sendValue = circle.get(sendKey);
			StringBuilder sock = new StringBuilder(); 
			try{
				sock.append(getSocketAddress(sendKey).toString());
			}catch(NoSuchElementException e){
				sock.append(sendKey);
			}
			String logSendValue = new String(sendValue);
			log.debug("Retrieving for Handler the pair: sendkey: {}, sendValue: {} ", sock, logSendValue );

			Map.Entry<ByteArrayWrapper, byte[]> entry = new AbstractMap.SimpleEntry<ByteArrayWrapper, byte[]>(sendKey, sendValue);
			
			//Use <Key,Value> entry or construct the byte stream of sending TS_PUT_PROCESS OR TS_REMOVE
			if(isSegmentRemove){
				////TODO: To be sent to other nodes through TS_REMOVE_PROCESS() to destinationNode;				
			}
			else{
				////TODO: To be sent to other nodes through TS_PUT_PROCESS() to destinationNode;
			}
			
			//TODO:
			//If choose to use Observer Pattern.
			// notifyViewers(){setChanged(); notifyObservers(entry)};
			//Alternatively,
			// Create a new interface for Handler to send the entry and InetSocket to Handlers.
			// Handler can instantiate appropriate type of Handler.
		}
		
		isComplete = true;
		
		return true;
	}
	
	/**
	 * "enables" a node in the membership protocol. 
	 * Node will be online if {@code key} is a participating node
	 * @param key ByteArrayWrapper of the node to "enable" in the membership protocol.
	 */
	public void enable(ByteArrayWrapper key) {
		//ByteArrayWrapper shutdownKeyOf = getNodeResponsible(key);
		String node = new String(mapOfNodes.get(key));

		int ret = listOfNodes.indexOf(key);
		if (-1 == ret){
			log.error(" ### ConsistentHashing::shutdown() key index not found for node {}", node );
			return;
		}
		membership.enable(ret);
	}
	
	/**
	 * Method to notify MemebershipProtocol to update the timestamp vector.
	 * @param node: key of the node that is to be "disabled".
	 */
	public void shutdown(ByteArrayWrapper key){
		log.debug(" *** ConsistentHashing::shutdown() CALLED ");
		if(key == null){
			log.info("     Shutdown of local node");
			membership.shutdown(null);
			return;
		}
		
		String node = new String(mapOfNodes.get(key));

		int ret = listOfNodes.indexOf(key);
		if (-1 == ret){
			log.error(" ### ConsistentHashing::shutdown() key index not found for node {}", node );
			return;
		}
		membership.shutdown(ret);
	}
	/**
	 * Obtains the closest predecessor node(IP address) on the Key-Value Store circle 
	 * "key" can be a key for a {@code node} or data.
	 * @param key	{@code key} whose predecessor to find.
	 * @return ByteArrayWrapper (key) for the predecessing node of {@code key}.
	 */
	private ByteArrayWrapper getPreviousNodeTo(ByteArrayWrapper key) {
		if (mapOfNodes.isEmpty()) {
			log.debug("Map Of Nodes Empty.");
			return null;
		}
		
			ByteArrayWrapper previousKey = key;
			SortedMap<ByteArrayWrapper, byte[]> headMap = mapOfNodes.headMap(key, false); //exclusive of {@code key}

			previousKey = headMap.isEmpty() ? mapOfNodes.tailMap(key, false).lastKey() : headMap.lastKey();
			
			return previousKey;
	}
	
	/** Helper method that obtains the predecessor node responsible/owner of a requested key with a valid timestamp
	 * @param requestedKey:	node or data whose predecessor to find.
	 * @return The node predecessor responsible for {@code requestedKey} which is deemed alive 
	 * by membership protocol. {@code null} if there aren't nodes alive.
	 */
	private ByteArrayWrapper getPreviousResponsible(ByteArrayWrapper requestedKey ){
		
		ByteArrayWrapper previousKey = getPreviousNodeTo(requestedKey);
		ByteArrayWrapper firstPreviousKey = previousKey;
		
		int time = this.membership.getTimestamp(listOfNodes.indexOf(previousKey));
		while( time < 0 ){
			previousKey = getPreviousNodeTo(previousKey);
			if( previousKey == firstPreviousKey){
				break;
			}
			time = this.membership.getTimestamp(listOfNodes.indexOf(previousKey));
		}
		String requestedWithKey;
		if( mapOfNodes.containsKey(requestedKey) ){
			requestedWithKey = getSocketAddress(requestedKey).toString();
			log.trace("Requested for Node: {}", requestedWithKey);
		}
		
		String previousNodeSocket = getSocketAddress(previousKey).toString();
		String timestamp = Integer.toString(time);
		
		log.trace("Predecessor Node: {} with timestamp: {}", previousNodeSocket, timestamp);
		
		return previousKey;
	}
	
	/**
	 * Obtains the predecessor node(IP address) that is responsible for the requested key in the Key-Value Store circle 
	 * The keys(arc) that a node is considered responsible for are (previousNode:exclusive, currentNode:inclusive].
	 * Helper method getPreviousResponsible() deems if a node is online/offline.
	 * @param requestedKey
	 * @return The node responsible for {@code requestedKey} as a InetSocketAddress.
	 */
	public InetSocketAddress getSocketPreviousResponsible(ByteArrayWrapper requestedKey) {
		log.trace("Finding InetSocketAddress of Predecessor.");

		ByteArrayWrapper previousKey = getPreviousResponsible(requestedKey);		

		return getSocketAddress(previousKey);
	}
	

	/**
	 * Accessor for data structure with view of the nodes in the Key-Value Store.
	 * @return 
	 */
	public SortedMap<ByteArrayWrapper, byte[]> getMapOfNodes() {
		return mapOfNodes;
	}
	
	/**
	 * Accessor for data structure with view of all pairs in the Key-Value Store.
	 * @return
	 */
	public Map<ByteArrayWrapper, byte[]> getCircle() {
		//if(IS_DEBUG) System.out.println("Size of circle @Accessor: "+circle.size());		
		return circle;
	}
	
	/**
	 * Method used to create the hashed ByteArrayWrapper of a given node 
	 * that is to be inserted in the Key-Value Store
	 * @param byte[] node
	 * @return ByteArrayWrapper for the hash of {@code byte[] node}
	 */
	public ByteArrayWrapper hashKey(byte[] node){
		ByteArrayWrapper key;
		byte[] digest;

		assert (node.length>0);
		try {
			digest = md.digest(node);
		} catch (ArrayIndexOutOfBoundsException e){
			e.printStackTrace();
			throw e;
		}
		
		if (digest.length > NodeCommands.LEN_KEY_BYTES) {
			key = new ByteArrayWrapper(Arrays.copyOfRange(digest, 0, NodeCommands.LEN_KEY_BYTES));
		} else {
			key = new ByteArrayWrapper( digest );
		}
		return key;
	}
	
	/**
	 * Method used to create the hashed ByteArrayWrapper of a given node 
	 * that is to be inserted in the Key-Value Store
	 * @param String node
	 * @return ByteArrayWrapper for the hash of {@code String node}
	 */
	public ByteArrayWrapper hashKey(String node){
	
		try {
			return hashKey(node.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			return hashKey(node.getBytes());
		}
	}
	
	@Override
	public void clear() {
		circle.clear();		
	}


	@Override
	public boolean containsKey(Object key) {
		boolean contain = false;
		try{
			contain = circle.containsKey(key);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
		return contain;
	}


	@Override
	public boolean containsValue(Object value) {
		boolean contain = false; 
		try{
			circle.containsValue(value);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
		
		return contain;
	}


	@Override
	public Set<Entry<ByteArrayWrapper, byte[]>> entrySet() {
		return circle.entrySet();
	}


	@Override
	public byte[] get(Object key) {
		byte[] fromGet = null;
		try{
			fromGet = get((ByteArrayWrapper) key);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
		return fromGet;
		
	}


	@Override
	public boolean isEmpty() {	
		return circle.isEmpty();
	}


	@Override
	public Set<ByteArrayWrapper> keySet() {
		return circle.keySet();
	}

	@Override
	public void putAll(Map<? extends ByteArrayWrapper, ? extends byte[]> m) {
		try{
			circle.putAll(m);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
	}

	@Override
	public byte[] remove(Object key) {
		byte[] fromRemove = null;
		try{
			fromRemove = remove((ByteArrayWrapper) key);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
		return fromRemove;
	}

	@Override
	public int size() {
		return circle.size();
	}


	@Override
	public Collection<byte[]> values() {
		return circle.values();
	}
	
	/**
	 * State of MembershipProtocol has changed. 
	 */
	@Override
	public void update(Observable o, Object arg) {
		
		//FIXME Correct log levels or Remove logging. Added for initial debugging purposes.
		String updatedArg = null;
		try{
			//TODO: We can choose to obtain different args from the Observable Membership protocol.
			updatedArg = (String) arg;
			
		}catch(ClassCastException e){
			String exception = e.getLocalizedMessage();
			log.error("ConsistHash. update() from MembershipProtocol. Casting Exception. {}", exception);
		} catch( Exception e ){
			String exception = e.getLocalizedMessage();
			log.error("ConsistHash. update() from MembershipProtocol. Exception {}", exception);
		}
		log.info("ConsistHash. update() in node {} received update from Memebership with response \"{}\"", getSocketAddress(localNode), updatedArg);
		
		
		// Since hasPredecessorChanged() updates the member variable onlinePredecessor, 
		// we need to save it here the non-changed onlinePredecessor to correctly transferKeys.
		ByteArrayWrapper firstKey = onlinePredecessor;

		if( hasPredecessorChanged() ){
			predecessorTransfer(firstKey);
		}
		
		hasReplicaChanged();
	}
	
	/**
	 * Detects differences in the currentOnline Predecessor and up-to-date Predecessor
	 * updates {@code onlinePredecessor} accordingly.
	 * @return true if differences were found, false if not. 
	 */
	private boolean hasPredecessorChanged() {
		boolean haschanged = false;
		ByteArrayWrapper newPredecessor = getPreviousResponsible(localNode);

		StringBuilder sb = new StringBuilder();
		sb.append("ConsistHash. on hasPredecessorChanged() predecessor old: ").append(getSocketAddress(onlinePredecessor))
			.append(" new: ").append(getSocketAddress(newPredecessor));
		
		//if(onlinePredecessor.equals(newPredecessor)){
		if(onlinePredecessor.compareTo(newPredecessor) == 0){
				haschanged = false;
		}else{
			haschanged = true;
			onlinePredecessor = newPredecessor;
		}
		sb.append(". hasChanged = " + haschanged);
		log.info(sb.toString());
		return haschanged;
	}

	/**
	 * TransferKeys in range (firstKey, localNode] (exclusive, inclusive].
	 * For our purposes of transferring: (onlinePredecesor.onlinePredecessor, onlinePredecesor] 
	 * hasPredecessorChanged() already updated {@code onlinePredecessor}, need non-changed onlinePredecessor. 
	 * @param firstKey key to begin transfers in range (firstKey, localNode]
	 */
	private void predecessorTransfer(ByteArrayWrapper firstKey) {
		//ByteArrayWrapper firstKey = getPreviousResponsible(onlinePredecessor); // Need non-changed onlinePredecessor.
		ByteArrayWrapper toKey = localNode;
		
		StringBuilder sb = new StringBuilder();
		sb.append("Transfer of keys in the range: (").append(getSocketAddress(firstKey))
		.append(", to ").append(getSocketAddress(toKey))
		.append("] to dest.node: ").append(getSocketAddress(onlinePredecessor));
		
		log.info("{}", sb.toString());
		
		transferSet  = transferKeys(firstKey, toKey); 
		createHandlerTransfers(getSocketAddress(onlinePredecessor), false);
		transferSet = null; //Clear transferSet after all Handlers have been created.
	}
	/**
	 * Detects differences in the old replica list with up-to-date replica list.
	 * Detects differences among all entries and starts transfer of keys.
	 * updates {@code localReplicaList} accordingly.
	 * @return true if differences were found, false if not. 
	 */
	private boolean hasReplicaChanged() {
				
		//FIXME Correct/improve logs, levels or Remove logging. Added for initial debugging purposes.

		boolean haschanged = false;
		
		ArrayList<InetSocketAddress> oldReplicas = new ArrayList<InetSocketAddress>(localReplicaList);
		List<InetSocketAddress> newReplicas = getReplicaList(localNode, true);
			
		if(newReplicas.equals(oldReplicas)){
			haschanged = false;
			//Debugging Sanity check, Logging here can be removed when not needed:
			log.info("ConsistHash. on hasReplicaChanged() did not detect change in old replica list of size({}) {} ", localReplicaList.size(), localReplicaList);
			log.info("ConsistHash. on hasReplicaChanged() did not detect change in new replica list of size({}) {} ", newReplicas.size(), newReplicas);

		}
		else{
			haschanged = true;
			//Debugging Sanity check, Logging here can be removed when not needed:
			log.info("ConsistHash. on hasReplicaChanged() replica list old: {} ", localReplicaList);
			log.info("ConsistHash. on hasReplicaChanged() replica list new: {} ", newReplicas);
		}
			
		replicaTransfer(oldReplicas, newReplicas);
		
		this.localReplicaList = getReplicaList(localNode, true);

		return haschanged;
	}

	/**
	 * Transfer Keys that belong to localNode to new entries in the replica list.
	 * Remove Keys that belong to localNode from the old(out-dated) entries of the replica list. 
	 * hasPredecessorChanged() already updated {@code onlinePredecessor}, need non-changed onlinePredecessor. 
	 * @param oldReplicas all entries of the out-dated replicas list.
	 * @param newReplicas all entries of the up-to-date replicas list.
	 */
	private void replicaTransfer(ArrayList<InetSocketAddress> oldReplicas,
			List<InetSocketAddress> newReplicas) {

		List<InetSocketAddress> tempNewReplicas = new ArrayList<InetSocketAddress>(newReplicas);

		// Find nodes that will receive keys, and nodes that will have keys removed.
		newReplicas.removeAll(oldReplicas); //Replicas that need to receive missing keys from this nodes.
		oldReplicas.removeAll(tempNewReplicas); //Replicas that need to remove keys of this node.
		
		log.info("ConsistHash. on replica TRANSFER to: {} ", newReplicas);

		//Key range for local Keys
		ByteArrayWrapper firstKey = onlinePredecessor;
		ByteArrayWrapper toKey = localNode;
		
		if(newReplicas.isEmpty() == false){
			//Populate Set for transfers.
			transferSet = transferKeys(firstKey, toKey);
			StringBuilder sb = new StringBuilder();
			String message = sb.append("Transfer of keys in (my) range: (").append(getSocketAddress( firstKey ))
			.append(", to ").append(getSocketAddress( toKey )).toString();
			log.trace("{}", sb.toString());
			
			for(InetSocketAddress sendToReplica : newReplicas){
				
				StringBuilder sb_node = new StringBuilder(message);
				sb_node.append("] to dest. node: ").append( sendToReplica );
		
				log.debug("{}", sb_node.toString());
				
				//Send Handlers to Transfer(send) local keys.
				createHandlerTransfers(sendToReplica, false); 
			}
			transferSet = null; //Clear transferSet after all Handlers have been created.

		}
		else{
			log.info("ConsistHash. No replicas found that need to sends key.");
		}
		
		log.info("ConsistHash. on replica REMOVE from: {} ", oldReplicas);
		if(oldReplicas.isEmpty() == false){
			//Populate Set for transfers.
			transferSet = transferKeys(firstKey, toKey);
			for(InetSocketAddress removeFromReplica : oldReplicas){
				//Send Handlers to Remove local keys.
				createHandlerTransfers(removeFromReplica, true);
			}
		}
		else{
			log.info("ConsistHash. No replicas found to that need keys removed.");
		}	
	}
	
	public static void main(String[] args) {
		//Testing: ConsistentHashing class.
		
		System.out.println("Testing ConsistentHashing.\nStarting...");

		String localnode = "Furry.local:11111";
		String[] nodes = {"planetlab2.cs.ubc.ca:11111",
				"cs-planetlab4.cs.surrey.sfu.ca:11111",
				"planetlab03.cs.washington.edu:11111",
				"pl1.csl.utoronto.ca:11111",
				localnode,
				"Knock3-Tablet:11111",
				"pl2.rcc.uottawa.ca:11111"};
				
		System.out.println();
		
		ConsistentHashing<ByteArrayWrapper, byte[]> ch = null;
		MembershipProtocol membership = null;
		ByteArrayWrapper nextNode, previousNode;
		
		try {
			//Setup ConsistentHashing & MembershipProtocol
			 ch = new ConsistentHashing<ByteArrayWrapper, byte[]>(nodes);
			 ch.setLocalNode(localnode);
			 int position = ch.getNodePosition(localnode);
			 membership = new MembershipProtocol(position, nodes.length, 0, 0);
			 ch.setMembership(membership);
			 
			 ch.num_replicas = 3;
			 membership.addObserver(ch);
			 
			 if(IS_DEBUG) System.out.println();
			 if(IS_DEBUG) System.out.println();
			 System.out.println("Consistent Hash created of node map size: "+ch.getMapOfNodes().size());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		System.out.println();

		
		try{
			System.out.println("Getting nodes...");
			
			SortedMap<ByteArrayWrapper, byte[]> map = ch.getMapOfNodes();
			Iterator<Entry<ByteArrayWrapper, byte[]>> iterator = map.entrySet().iterator();
			
			//Testing: Retrieval of nodes in the map.
			System.out.println("Contains "+map.size()+" nodes... ");
				while(iterator.hasNext()){
					Entry<ByteArrayWrapper, byte[]> e = iterator.next();
					System.out.println("(Key,Value): "+e.getKey() +" "+ new String(e.getValue()) );
				}
				
				System.out.println();
				System.out.println("======== Local Node: " + localnode + " ========");
			
				//Increment timestamps.
				int time = 0;
				int someTimeLater = 50;
				System.out.println();
				System.out.println("Simulating incrementing timestamps of the nodes.");
				System.out.println("=========================");
				System.out.println(" === timestamp vector before "+ membership.incrementAnEntry() );
				System.out.println(" .." + (int) (someTimeLater + 1) + " events later.. ");
				
				//showAllNodes(ch, map);
				
				while(time++ < someTimeLater){
					membership.incrementAnEntry();
				}
				System.out.println(" === timestamp vector after  "+ membership.incrementAnEntry() );
				
				String someValue = "some key here";
				ch.put(ch.hashKey("some key here"), someValue.getBytes());
				
				//Shutdown
				//Test1:
				int num_shutdown = 3;
				List<Integer> shutdownindeces = new ArrayList<Integer>(num_shutdown);
				System.out.println();
				System.out.println("Simulating random shutdown of "+num_shutdown+" nodes.");
				System.out.println("=========================");
				int j = 0;
				//while( j < num_shutdown && num_shutdown < nodes.length){
				///while( j >= 0  && j > nodes.length -1 -num_shutdown){ // Use j=0; j--; Shutdown consecutive num_shutdown number of nodes from the end.	
				//	Integer index = membership.getRandomIndex(); // =j;//Determine order of node shutdown.
				//	if(index == membership.current_node){
				//		break;
				//	}
				//	boolean success = membership.shutdown(index); //Shutdown
				//	System.out.println("The index shutdown was previously online: "+success);
				//	shutdownindeces.add(index);
				//	j++;
				//}
				
				//Test2a:
				System.out.println();
				System.out.println("Simulating shutdown of nodes in some order.");
				System.out.println("=========================");
				// Shutdown all nodes in some sequential order.
				for(j = 0; j < nodes.length; j++){
					membership.shutdown( j );
					shutdownindeces.add(j);
				}
				// Reenable nodes
				//membership.enable(6);
				// Nodes to be reenabled at a later time. Simulates "detected offline"
				//shutdownindeces.add(6);

				// Testing: given a provided key, locate the successor("next") key.
			String looking_for_next_of ;//= cs-planetlab4.cs.surrey.sfu.ca:11111";
										//"pl1.csl.utoronto.ca";
										//"planetlab03.cs.washington.edu";
			
			System.out.println();
			System.out.println("Locating Nodes in Key-Value store.");
			System.out.println("=========================");

						
			showAllNodes(ch, map);
				

				//Enable node online
				int num_enabled = 3;
				System.out.println();
				System.out.println("Simulating enable of ("+num_enabled+" or "+shutdownindeces.size()+") nodes.");
				System.out.println("=========================");
				int k = 0;
				while( k < num_enabled && k < nodes.length && k < shutdownindeces.size()){
					Integer index = shutdownindeces.remove(0);
					boolean success = membership.enable(index); //enable
					System.out.println("The index enabled was previously offline: "+success);
					k++;
				}
		//		System.out.println("Total enabled: "+k);

			
			if(num_enabled != 0){
				System.out.println();
				System.out.println("Locating Nodes in Key-Value store after enable.");
				System.out.println("=========================");

				showAllNodes(ch, map);
			}
				
		}
		catch(ConcurrentModificationException cme){
			// synchronized(){......} should occur before using the iterator.
			cme.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * Helper method used by ConsistentHashing.main() for testing functionality of {@code ConsistentHashing}.
	 * Contains implementation to display online Successor, online Predecessor and online ReplicaList.
	 * @param ch ConsistentHashing has been setup with
	 * @param map of nodes to generate iterator.
	 */
	private static void showAllNodes(
			ConsistentHashing<ByteArrayWrapper, byte[]> ch,
			SortedMap<ByteArrayWrapper, byte[]> map) {
		
		ByteArrayWrapper previousNode, nextNode;
		
		Iterator<Entry<ByteArrayWrapper, byte[]>> iterator;
		String looking_for_next_of;
		iterator = map.entrySet().iterator();
		
		while(iterator.hasNext()){
			Entry<ByteArrayWrapper, byte[]> e = iterator.next();

			looking_for_next_of = ch.getSocketNodeResponsible(e.getKey()).toString();
			System.out.println("From perspective of node (Key,Value): "+e.getKey() +" [value->"+ new String(e.getValue()) +"]");
			
			previousNode = ch.getPreviousResponsible( e.getKey() );
			nextNode = ch.getNextNodeTo( e.getKey() );

			System.out.println("Online Predecessor: "+ ch.getSocketPreviousResponsible(e.getKey()) );
			System.out.println("Online Successor: "+ ch.getSocketNodeResponsible(e.getKey()) );

			List<InetSocketAddress> list = ch.getReplicaList(e.getKey(), false);
			System.out.println("Online Replicas main(): "+list);
			System.out.println();
		}
	}

	
}
