package com.b6w7.eece411.P02.nio;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;


public class ConsistentHashing<K, V> implements Map<ByteArrayWrapper, byte[]>{

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
	
	/*** Maintains the current list of nodes that host replicas for primary key-values that belong to localhost*/
	private final LinkedList<InetSocketAddress> localReplicaList = new LinkedList<InetSocketAddress>();
	
	/**The structure used to maintain in sorted order the Keys that are present in the local Key-Value store.*/
	private PriorityQueue<ByteArrayWrapper> orderedKeys;
	
	/**Variable used to maintain identity of the key for the localhost(local ServiceReactor).
	 * FIXME: Ideally, a final variable.
	 */
	private ByteArrayWrapper localNode;

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

			byte[] fromMap = mapOfNodes.put(key, node.getBytes()); //circle.put(key, node.getBytes());			
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
		orderedKeys = new PriorityQueue<ByteArrayWrapper>((int) (40000*1.2)); //Initialized to large capacity to avoid excessive resizing.
		orderedKeys.addAll(circle.keySet());
	}
	
	public void setMembership(MembershipProtocol membership) {
		this.membership = membership;
	}
	
	public void setLocalNode(String key) {
		localNode = getOwner(hashKey(key));	
		log.debug("setLocalNode(): {} {}",key, localNode);		
	}
	
	/**
	 * 
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
		orderedKeys.add( key );
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
	 * Obtains the InetSocketAddress of a random node that MemebrshipProtocol views online.
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
	 * The keys that a node is responsible for are (previousNode:exclusive, currentNode:inclusive].
	 * ..Similar to using getOwner() + getSockAddress() successively.
	 * Does not check if the node is online/offline.
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
		List<InetSocketAddress> replicas = updateLocalReplicaList();
		// TODO
		// for now just pass through, but we might want to be saving it here, and just before
		// saving, comparing it to the old copy.  If there is a difference, then a transfer of 
		// keys is in order.
		return new LinkedList<InetSocketAddress>(replicas);
	}

	private List<InetSocketAddress> updateLocalReplicaList() {
		List<InetSocketAddress> replicas = new LinkedList<InetSocketAddress>();
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
		FIND_REPLICAS: while (!nextInet.equals(startingInet) && numReplicasLeftToAdd > 0) {
			
//			if (membership.getTimeout(nextInet) > 0) {
//				// This node is online, so we can use it
//				// This node is online, so we add to replicaList
//				replicas.add(nextInet);
//				numReplicasLeftToAdd --;
//			}

			// No need to check for online because getSocketNodeResponsible does that already
			replicas.add(nextInet);
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
	 * @param removeSelf TODO
	 * @return
	 */
	public List<InetSocketAddress> getReplicaList(ByteArrayWrapper requestedKey, boolean removeSelf) {
		List<InetSocketAddress> replicas = new ArrayList<InetSocketAddress>(num_replicas+1);
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
		
		return replicas;
	}
	
	/**
	 * Helper method to obtain the InetSocketAddress for a key(that represents a node entry)
	 * @param node
	 * @return InetSocketAddress of {@code node} 
	 */
	private InetSocketAddress getSocketAddress(ByteArrayWrapper node){
		String addr[] = (new String(mapOfNodes.get(node))).split(":");
		
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
	
	/** Helper method that obtains the node responsible/owner of a requested key with a valid timestamp
	 * @param requestedKey:	node or data whose owner to find.
	 * @return The node responsible for {@code requestedKey} which is deemed alive 
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

		ByteArrayWrapper tempNextKey = nextKey;
		int x = this.membership.getTimestamp(listOfNodes.indexOf(nextKey));
		while(x < 0){
			nextKey = getNextNodeTo(nextKey);
			if( nextKey == tempNextKey ){
				byte[] get = circle.get(requestedKey);
				if(null != get) {
					String nextOfValue = new String(get);
					log.warn("#### Get(Remote)NodeResponsible has returned localnode{}[value->{}", nextKey.toString(), nextOfValue);
				}
				break;
			}		
			x = this.membership.getTimestamp(listOfNodes.indexOf(nextKey));
		}
		
		return nextKey;
	}
	
	/**
	 * Obtains the closest node(IP address) on the Key-Value Store circle 
	 * that is subsequent to the given key.
	 * "key" can be a key for a node or data.
	 * @param key	ByteArrayWrapper
	 * @return ByteArrayWrapper (key) for the next node of {@code key}.
	 */
	public ByteArrayWrapper getNextNodeTo(ByteArrayWrapper key) {
		if (mapOfNodes.isEmpty()) {
			log.debug("Map Of Nodes Empty.");
			return null;
		}
		
			ByteArrayWrapper nextKey;
			SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(key);
			nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();
			
			if (tailMap.containsKey(key) ==  true) {
				log.trace("** Key exists in circle. {}", NodeCommands.byteArrayAsString(nextKey.key) ) ;
				//if(tailMap.isEmpty() == false){
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
	 * Method that populates a ByteBuffer with (Key,Value) pairs so that they may be transferred to other nodes.
	 * Used when joining/leaving the Key-Value Store.
	 * @param out: (ByteBuffer used to send the Key-Value pairs).
	 * @param keyLimit: (ByteArrayWrapper that will determine the keys to be transferred [inclusive limit].
	 */
	public void transferKeys(ByteBuffer out, ByteArrayWrapper keyLimit){
	/**TODO: Untested */
		boolean transfersComplete = false;
		int compare = 0;//	int i = 0;
		byte iBytes = 0; int index = out.position();
		
		//byte[] iBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(i).array();
		out.put(iBytes); 

		ByteArrayWrapper peekKey = orderedKeys.peek();
		while(peekKey != null && (compare = keyLimit.compareTo(peekKey)) <=0){
			if(out.remaining() >= NodeCommands.LEN_KEY_BYTES + NodeCommands.LEN_VALUE_BYTES ){
				peekKey = orderedKeys.poll();
				out.put(peekKey.keyBuffer);
				out.put(mapOfNodes.get(peekKey));
				iBytes++;
			}
			else{
				transfersComplete = false;
				break;
			}
			peekKey = orderedKeys.peek();
		}
		
		transfersComplete = (peekKey == null || compare >=0);
		
		//iBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(i).array();
		out.put(index, iBytes);
		out.flip();	
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
	
	
	public static void main(String[] args) {
		//Testing: ConsistentHashing class.
		
		System.out.println("Testing ConsistentHashing.\nStarting...");

		String[] nodes = {"planetlab2.cs.ubc.ca:11111",
				"cs-planetlab4.cs.surrey.sfu.ca:11111",
				"planetlab03.cs.washington.edu:11111",
				"pl1.csl.utoronto.ca:11111",
				"Furry.local:11111",
				"dhcp-128-189-79-19.ubcsecure.wireless.ubc.ca:11111",
				"pl2.rcc.uottawa.ca:11111"};
				
		System.out.println();
		
		ConsistentHashing<ByteArrayWrapper, byte[]> ch = null;
		
		try {
			 ch = new ConsistentHashing<ByteArrayWrapper, byte[]>(nodes);
			 ch.setLocalNode("Furry.local:11111");
			 
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
			
			SortedMap<ByteArrayWrapper, byte[]> mn = ch.getMapOfNodes();
			Iterator<Entry<ByteArrayWrapper, byte[]>> is = mn.entrySet().iterator();
			
			//Testing: Retrieval of nodes in the map.
			System.out.println("Got nodes... "+mn.size());
			synchronized(ch){
				while(is.hasNext()){
					Entry<ByteArrayWrapper, byte[]> e = is.next();
					System.out.println("(Key,Value): "+e.getKey() +" "+ new String(e.getValue()) );
				}
			}
			// Testing: given a provided key, locate the subsequent("next") key.
			String looking_for_next_of = "cs-planetlab4.cs.surrey.sfu.ca:11111";
										//"pl1.csl.utoronto.ca";
										//"planetlab03.cs.washington.edu";
			
			System.out.println();
			System.out.println("Locating a Neighbor of key in the map.");

			synchronized(ch){
				is = mn.entrySet().iterator();
				while(is.hasNext()){
					System.out.println("Locating the Neighbour IP address of a given key. " 
							+ looking_for_next_of
							+ " " + ch.hashKey(looking_for_next_of)+"]"
							);
					Entry<ByteArrayWrapper, byte[]> e = is.next();
					if( ch.hashKey(looking_for_next_of).equals(e.getKey()) ) 
						System.out.println("Current Key & NextOfTarget key are the same.");
					System.out.println("From node (Key,Value): "+e.getKey() +" [value->"+ new String(e.getValue()) +"]");
					System.out.println("Neighbour:"+ch.getNextNodeTo( ch.hashKey(looking_for_next_of) ) );
					
					List<InetSocketAddress> list = ch.getReplicaList(e.getKey(), false);
					System.out.println("Replicas main(): "+list);
					System.out.println();
				}
			}
			
			//List<InetSocketAddress> list = ch.getSocketReplicaList(requestedKey);
			
			//Membership.Current_Node = ch.getClosestNodeTo( hashKey(InetAddress.getLocalHost()) );
		}
		catch(ConcurrentModificationException cme){
			// synchronized(){......} should occur before using the iterator.
			cme.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
