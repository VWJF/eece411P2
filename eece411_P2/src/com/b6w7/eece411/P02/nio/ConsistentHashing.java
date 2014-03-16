package com.b6w7.eece411.P02.nio;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;

// Based from code:
// https://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html
public class ConsistentHashing implements Map{

	public static boolean IS_DEBUG = false;
	public static boolean IS_VERBOSE = false;

//	private final HashFunction hashFunction;
//	private final int numberOfReplicas;
	/**
	 * The structure used to maintain the view of the pairs (key,value) & participating nodes. 
	 */
	private final SortedMap<ByteArrayWrapper, byte[]> circle;// = new TreeMap<ByteArrayWrapper, byte[]>();
	/**
	 * The structure used to maintain the view of the participating nodes. 
	 */
	private final SortedMap<ByteArrayWrapper, byte[]> mapOfNodes;// = new TreeMap<ByteArrayWrapper, byte[]>();

//	private final SortedMap<ByteArrayWrapper, byte[]> circleOfNodes = new TreeMap<ByteArrayWrapper, byte[]>();
//	private List<Entry<ByteArrayWrapper,byte[]>> listOfNodes;

	//private ByteArrayWrapper key;
	private static MessageDigest md;

	public ConsistentHashing(String[] nodes) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		//this.key = baw;
//		this.numberOfReplicas = numberOfReplicas;

//		for (T node : nodes) {
//			add(node);
//		}
		
		//TODO: Ideally, the map(circle) has been initialized to store Commands.MAX_MEMORY (40,000) entries
		// so that resizing does not need to occur.
		this.circle = Collections.synchronizedSortedMap(new TreeMap<ByteArrayWrapper, byte[]>());
		this.mapOfNodes = Collections.synchronizedSortedMap(new TreeMap<ByteArrayWrapper, byte[]>());
		this.md = MessageDigest.getInstance("SHA-1");

		//ByteArrayWrapper key;
//		listOfNodes = Collections.synchronizedList(new ArrayList<Entry<ByteArrayWrapper,byte[]>>(Command.MAX_NODES));
		
		int i = 0;
		for (String node : nodes){	
			if(IS_DEBUG) System.out.println(i +"."+ node);
			byte[] digest = md.digest(node.getBytes());
			
			if(IS_DEBUG) System.out.println("digest: "+NodeCommands.byteArrayAsString(digest));
			//ByteArrayWrapper key = hashKey(digest);
			ByteArrayWrapper key = hashKey(node);
			if(IS_DEBUG) System.out.println("hashKey: "+key);

			put(key, node.getBytes()); //circle.put(key, node.getBytes());			
			String err_msg = (null == get(hashKey(node))) ? "****Fail to get" : "Success returned get(): "+
					NodeCommands.byteArrayAsString(get(hashKey(node)));
			
			if(IS_DEBUG) System.out.println(err_msg);
			if(IS_DEBUG) System.out.println("Circle Size: "+circle.size());
			if(IS_DEBUG) System.out.println(i +"."+ hashKey(node)+" "+new String(get(key)));
			if(IS_DEBUG) System.out.println();
			i++;
		}
		
		System.out.println("Size at Constructor: "+circle.size());

		// circle has been initialized with the pairs of (Node, IP address).
		// Create a new view containing only the existing nodes.
		mapOfNodes.putAll( circle.tailMap(circle.firstKey()) );

//		Storing view of nodes in a List with Iterator:
//		Iterator<Entry<ByteArrayWrapper,byte[]>> is = circle.entrySet().iterator();
//		while (is.hasNext()){
//			listOfNodes.add(is.next());
//		}
		
//		Storing view of nodes in a List with Loop
//		Set<Entry<ByteArrayWrapper,byte[]>> nodes1 = circle.entrySet();	
//		for(Entry<ByteArrayWrapper,byte[]> node : nodes1){
//			listOfNodes.add(node);
//		}
	}
	

	public byte[] put(ByteArrayWrapper key, byte[] value) {
		//   for (int i = 0; i < numberOfReplicas; i++) {
		//     circle.put(key.hash(node.toString() + i), node);
		//   }
		
		
		// Additional Checking unnecessary since the thread 
		// using the Map should impose additional restrictions.
		//if(circle.size() == Command.MAX_MEMORY && circle.containsKey(key)){
		return circle.put(key, value);
	}

	public byte[] remove(ByteArrayWrapper key) {
		//   for (int i = 0; i < numberOfReplicas; i++) {
		//     circle.remove(key.hash(node.toString() + i));
		//   }
		return circle.remove(key);
	}

	
	public byte[] get(ByteArrayWrapper key) {
		if (circle.isEmpty()) {
			return null;
		}
		return circle.get(key);
	}
	
	/**
	 * Obtains the closest node(IP address) on the Key-Value Store circle 
	 * that is subsequent to the given key.
	 * key can be a key for a node or data.
	 * @param key
	 * @return
	 */
	public InetAddress getClosestNodeTo(ByteArrayWrapper key) {
		if (mapOfNodes.isEmpty()) {
			System.out.println("Map Of Nodes Empty.");
			return null;
		}
		
			ByteArrayWrapper nextKey; // = key.hash(key);
			SortedMap<ByteArrayWrapper, byte[]> tailMap = mapOfNodes.tailMap(key);
			nextKey = tailMap.isEmpty() ? mapOfNodes.firstKey() : tailMap.firstKey();
			
			if (tailMap.containsKey(key) ==  true) {
				if(IS_VERBOSE) System.out.println("** Key exists in circle. "+NodeCommands.byteArrayAsString(nextKey.getData()));
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
			

			try {
				if(IS_VERBOSE) System.out.println("Finding InetAddress.");
				return InetAddress.getByName(nextHost);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				System.out.println("## getNext node in circle exception. " + e.getLocalizedMessage());
				e.printStackTrace();
			}
		return null;
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
	public SortedMap<ByteArrayWrapper, byte[]> getCircle() {
		System.out.println("Size of circle @Accessor: "+circle.size());
		
		return circle;
	}
	/**
	 * Method used to create the hashed ByteArrayWrapper of a given node 
	 * that is to be inserted in the Key-Value Store
	 * @param node
	 * @return
	 */
	public static ByteArrayWrapper hashKey(String node){
		ByteArrayWrapper key;
		byte[] digest;
		
		try {
			digest = md.digest(node.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			digest = md.digest(node.getBytes());
		}

		if (digest.length > NodeCommands.LEN_KEY_BYTES) {
			key = new ByteArrayWrapper(Arrays.copyOfRange(digest, 0, NodeCommands.LEN_KEY_BYTES));
		} else {
			key = new ByteArrayWrapper( digest );
		}
		return key;
	}


	
	public static void main(String[] args) {

		System.out.println("Starting...");

		String[] nodes = {"planetlab2.cs.ubc.ca",
				"cs-planetlab4.cs.surrey.sfu.ca",
				"planetlab03.cs.washington.edu",
				"pl1.csl.utoronto.ca",
				"pl2.rcc.uottawa.ca"};
		
		System.out.println();
		
		ConsistentHashing ch = null;
		
		try {
			 ch = new ConsistentHashing(nodes);
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
			System.out.println("Locating a \"Next\" key in the map.");

			synchronized(ch){
				is = mn.entrySet().iterator();
				while(is.hasNext()){
					System.out.println("Locating the Next IP address of a given key. " 
							+ looking_for_next_of
							+ " " + hashKey(looking_for_next_of)+"]"
							);
					Entry<ByteArrayWrapper, byte[]> e = is.next();
					if( hashKey(looking_for_next_of).equals(e.getKey()) ) 
						System.out.println("Current Key & NextOfTarget key are the same.");
					System.out.println("From node (Key,Value): "+e.getKey() +" [value->"+ new String(e.getValue()) +"]");
					System.out.println("Next:"+ch.getClosestNodeTo( hashKey(looking_for_next_of) ) );
					System.out.println();
				}
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


	@Override
	public void clear() {
		circle.clear();		
	}


	@Override
	public boolean containsKey(Object key) {
		return circle.containsKey(key);
	}


	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public Set<Entry<ByteArrayWrapper, byte[]>> entrySet() {
		return circle.entrySet();
	}


	@Override
	public Object get(Object key) {
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
	public Object put(Object key, Object value) {
		
		byte[] fromPut = null;
		try{
			fromPut = put((ByteArrayWrapper) key, (byte[]) value);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
		return fromPut;
	}


	@Override
	public void putAll(Map m) {
		// TODO Auto-generated method stub
		try{
			circle.putAll(m);
		}catch(ClassCastException cce){
			cce.printStackTrace();
		}
	}


	@Override
	public Object remove(Object key) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return circle.size();
	}


	@Override
	public Collection<byte[]> values() {
		return circle.values();
	}
	
}
