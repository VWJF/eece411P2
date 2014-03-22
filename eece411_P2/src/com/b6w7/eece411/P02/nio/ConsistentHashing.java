package com.b6w7.eece411.P02.nio;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;

//Based from code:
// https://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html

public class ConsistentHashing<TK, TV> implements Map<ByteArrayWrapper, byte[]>{

	public static boolean IS_DEBUG = true;
	public static boolean IS_VERBOSE = false;

//	private final HashFunction hashFunction;
//	private final int numberOfReplicas;
	
	/**
	 * The structure used to maintain the view of the pairs (key,value) & participating nodes.
	 * Should be initialized to MAX_MEMORY*(1/load_factor), to limit the number of keys & avoid resizing.
	 */
	private HashMap<ByteArrayWrapper, byte[]> circle; //new HashMap<ByteArrayWrapper, byte[]>((int)(40000*1.2))
	//private final SortedMap<ByteArrayWrapper, byte[]> circle; // = new TreeMap<ByteArrayWrapper, byte[]>();
	
	/**
	 * The structure used to maintain the view of the participating nodes. 
	 */
	private final TreeMap<ByteArrayWrapper, byte[]> mapOfNodes; // = new TreeMap<ByteArrayWrapper, byte[]>();

	/**
	 * The structure used to maintain the record of the natural ordering within the map of nodes.
	 */
	private final List<ByteArrayWrapper> listOfNodes;
	
	/**
	 * The structure used to maintain in sorted order the Keys that are present in the local Key-Value store.
	 */
	private PriorityQueue<ByteArrayWrapper> orderedKeys;

	private MembershipProtocol membership;
	private static MessageDigest md;

	public ConsistentHashing(String[] nodes) throws NoSuchAlgorithmException, UnsupportedEncodingException {

//		for (T node : nodes) {
//			add(node);
//		}
		this.circle = new HashMap<ByteArrayWrapper, byte[]>((int)(40000*1.2)); //Initialized to large capacity to avoid excessive resizing.
		this.mapOfNodes = new TreeMap<ByteArrayWrapper, byte[]>();
		this.md = MessageDigest.getInstance("SHA-1");
		
		int i = 0;
		for (String node : nodes){	
			if(IS_DEBUG) System.out.println(i +"."+ node);
			byte[] digest = md.digest(node.getBytes());
			
			if(IS_DEBUG) System.out.println("     ConsistentHashing() digest: "+NodeCommands.byteArrayAsString(digest));
			//ByteArrayWrapper key = hashKey(digest);
			ByteArrayWrapper key = hashKey(node);
			if(IS_DEBUG) System.out.println("     ConsistentHashing() hashKey: "+key);

			mapOfNodes.put(key, node.getBytes()); //circle.put(key, node.getBytes());			
			String err_msg = (null == getNode(hashKey(node))) ? " *** ConsistentHashing() Fail to get" : "     ConsistentHashing() Success returned get(): "+
					NodeCommands.byteArrayAsString(getNode(hashKey(node)));
			
			if(IS_DEBUG) System.out.println(err_msg);
			if(IS_DEBUG) System.out.println("     ConsistentHashing() Map Of Nodes Size: "+mapOfNodes.size());
			if(IS_DEBUG) System.out.println("     ConsistentHashing() " + i +"."+ hashKey(node)+" "+new String(getNode(key)));
			if(IS_DEBUG) System.out.println();
			i++;
		}
		
		System.out.println("     ConsistentHashing() Map of Nodes Size at Constructor: "+mapOfNodes.size());
		
		listOfNodes = new ArrayList<ByteArrayWrapper>(mapOfNodes.keySet());
		Collections.sort(listOfNodes);

		orderedKeys = new PriorityQueue<ByteArrayWrapper>((int) (40000*1.2)); //Initialized to large capacity to avoid excessive resizing.
		orderedKeys.addAll(circle.keySet());
		// circle has been initialized with the pairs of (Node, hostname).
		// Create a new view containing only the existing nodes.
		// mapOfNodes.putAll( circle.tailMap(circle.firstKey()) );
	}
	
	public void setMembership(MembershipProtocol membership) {
		this.membership = membership;
	}
	
	private byte[] addNode(ByteArrayWrapper key, byte[] value) {
		
		// Additional Checking unnecessary since the thread that
		// uses the Map should impose additional restrictions.
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
	 * @param node
	 * @return
	 */
	public int getNodePosition(String node){
		ByteArrayWrapper key = hashKey(node);
		if(IS_DEBUG) System.out.println("     ConsistentHashing::getNodePosition()  hashKey: "+key);
		int ret = listOfNodes.indexOf(key);
		if (-1 == ret)
			System.out.println(" ### ConsistentHashing::getNodePosition() index not found for node "+ node);
		return ret;
	}
	
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
	
	public InetSocketAddress getRandomOnlineNode() {
		if(IS_VERBOSE) System.out.println("     ConsistentHashing.getRandomOnlineNode()");
		if (mapOfNodes.isEmpty()) {
			if(IS_VERBOSE) System.out.println(" ### ConsistentHashing.getRandomOnlineNode() Map of Nodes Empty.");
			return null;
		}
		
		int randomIndex = membership.getRandomIndex();
		if(IS_VERBOSE) System.out.println("     ConsistentHashing.getRandomOnlineNode() Index: "+randomIndex);
		ByteArrayWrapper node = listOfNodes.get(randomIndex);

		// we found a random element
		String nextHost = new String(mapOfNodes.get(node));
		if(IS_VERBOSE) System.out.println("     ConsistentHashing.getRandomOnlineNode() CHOSEN    " + nextHost);

		String addr[] = nextHost.split(":");
		return new InetSocketAddress(addr[0], Integer.valueOf(addr[1]));
	}
	
	/**
	 * Obtains the node(IP address) that is responsible for the requested key in the Key-Value Store circle 
	 * The keys that a node is responsible for are (previousNode:exclusive, currentNode:inclusive].
	 * @param requestedKey
	 * @return
	 */
	public InetSocketAddress getNodeResponsible(ByteArrayWrapper requestedKey) {
		if (mapOfNodes.isEmpty()) {
			System.out.println("Map Of Nodes Empty.");
			return null;
		}

		ByteArrayWrapper nextKey;
		SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(requestedKey);
		nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();

		/**TODO: Addition get Next node responsible.
		 * Untested.
		 * */ 	
		ByteArrayWrapper tempNextKey = getNextNodeTo(nextKey);
		nextKey = tempNextKey;
		int x = this.membership.getTimestamp(listOfNodes.indexOf(nextKey));
		while(x < 0){
			nextKey = getNextNodeTo(nextKey);
			if( nextKey == tempNextKey ){
				if(IS_VERBOSE) {
					String nextOfValue = new String(circle.get(requestedKey));
					System.out.println("#### Get(Remote)NodeResponsible has returned localnode"+nextKey.toString()+"[value->"+nextOfValue);
				}
				break;
			}		
			x = this.membership.getTimestamp(listOfNodes.indexOf(nextKey));
		}
		
		String nextHost = new String(mapOfNodes.get(nextKey));
		String nextOfValue = "(key,value) does not exist in circle";
		if(circle.get(requestedKey)!= null)
			nextOfValue = new String(circle.get(requestedKey));
		if(IS_VERBOSE) System.out.println("NextOf: "+requestedKey.toString()+"[value->"+nextOfValue
				+"]"+"\nis target TargetHost: "+nextKey+" [value->"+nextHost+"]");

		String addr[] = nextHost.split(":");

		if(IS_VERBOSE) System.out.println("Finding InetSocketAddress.");
		return new InetSocketAddress(addr[0], Integer.valueOf(addr[1]));
	}
	
	/**
	 * Obtains the closest node(IP address) on the Key-Value Store circle 
	 * that is subsequent to the given key.
	 * "key" can be a key for a node or data.
	 * @param key
	 * @return ByteArrayWrapper (Key) for the next node.
	 */
	public ByteArrayWrapper getNextNodeTo(ByteArrayWrapper key) {
		if (mapOfNodes.isEmpty()) {
			System.out.println("Map Of Nodes Empty.");
			return null;
		}
		
			ByteArrayWrapper nextKey;
			SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(key);
			nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();
			
			if (tailMap.containsKey(key) ==  true) {
				if(IS_VERBOSE) System.out.println("** Key exists in circle. "+NodeCommands.byteArrayAsString(nextKey.key) ) ;
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
			if(IS_VERBOSE) System.out.println("NextOf: "+key.toString()+"[value->"+nextOfValue
							+"]"+"\nis target TargetHost: "+nextKey+" [value->"+nextHost+"]");		

//			try {
//				if(IS_VERBOSE) System.out.println("Resolving InetAddress.");
//				return InetAddress.getByName(nextHost);
//			} catch (UnknownHostException e) {
//				System.out.println("## getNext node in circle exception. " + e.getLocalizedMessage());
//				e.printStackTrace();
//			}
//		return null;
		return nextKey;

	}
	
	// isThisMyIpAddress() code obtained and modified from 
	// http://stackoverflow.com/questions/2406341/how-to-check-if-an-ip-address-is-the-local-host-on-a-multi-homed-system
	public static boolean isThisMyIpAddress(InetSocketAddress owner, int port){
	    // Check if the address is defined on any interface
		try {
			return (owner.getAddress().toString().equals(InetAddress.getLocalHost().toString())) && owner.getPort() == port;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return false;
		}
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
	 * Method to notify MemebershipProtocol to update the timestamp vector.
	 * @param node: key of the node that is to be "disabled".
	 */
	public void shutdown(ByteArrayWrapper key){
		if(key == null){
			membership.shutdown(null);
			return;
		}
		
		//ByteArrayWrapper key = hashKey(node);
		String node = new String(mapOfNodes.get(key));

		if(IS_DEBUG) System.out.println("     ConsistentHashing::shutdown() key: "+node);
		
		int ret = listOfNodes.indexOf(key);
		if (-1 == ret){
			System.out.println(" ### ConsistentHashing::shutdown() key index not found for node "+ node );
			return;
		}
		membership.shutdown(ret);
	}
	/**
	 * Accessor for data structure with view of the nodes in the Key-Value Store.
	 * @return
	 */
	public SortedMap<ByteArrayWrapper, byte[]> getMapOfNodes() {
		System.out.println("Size of node map @Accessor: "+mapOfNodes.size());
		return mapOfNodes;
	}
	
	/**
	 * Accessor for data structure with view of all pairs in the Key-Value Store.
	 * @return
	 */
	public Map<ByteArrayWrapper, byte[]> getCircle() {
		System.out.println("Size of circle @Accessor: "+circle.size());		
		return circle;
	}
	
	/**
	 * Method used to create the hashed ByteArrayWrapper of a given node 
	 * that is to be inserted in the Key-Value Store
	 * @param byte[] node
	 * @return
	 */
	public static ByteArrayWrapper hashKey(byte[] node){
		ByteArrayWrapper key;
		byte[] digest;

		digest = md.digest(node);
		
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
	 * @return
	 */
	public static ByteArrayWrapper hashKey(String node){
	
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


/*	@Override
	public byte[] put(Object key, Object value) {
		
		byte[] fromPut = null;
		try{
			fromPut = put((ByteArrayWrapper) key, (byte[]) value);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
		return fromPut;
	}
*/

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

		String[] nodes = {"planetlab2.cs.ubc.ca",
				"cs-planetlab4.cs.surrey.sfu.ca",
				"planetlab03.cs.washington.edu",
				"pl1.csl.utoronto.ca",
				"pl2.rcc.uottawa.ca"};
				
		System.out.println();
		
		ConsistentHashing<ByteArrayWrapper, byte[]> ch = null;
		
		try {
			 ch = new ConsistentHashing<ByteArrayWrapper, byte[]>(nodes);
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
			String looking_for_next_of = "cs-planetlab4.cs.surrey.sfu.ca";
										//"pl1.csl.utoronto.ca";
										//"planetlab03.cs.washington.edu";
			
			System.out.println();
			System.out.println("Locating a Neighbor of key in the map.");

			synchronized(ch){
				is = mn.entrySet().iterator();
				while(is.hasNext()){
					System.out.println("Locating the Neighbour IP address of a given key. " 
							+ looking_for_next_of
							+ " " + hashKey(looking_for_next_of)+"]"
							);
					Entry<ByteArrayWrapper, byte[]> e = is.next();
					if( hashKey(looking_for_next_of).equals(e.getKey()) ) 
						System.out.println("Current Key & NextOfTarget key are the same.");
					System.out.println("From node (Key,Value): "+e.getKey() +" [value->"+ new String(e.getValue()) +"]");
					System.out.println("Neighbour:"+ch.getNextNodeTo( hashKey(looking_for_next_of) ) );
					System.out.println();
				}
			}
			
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
