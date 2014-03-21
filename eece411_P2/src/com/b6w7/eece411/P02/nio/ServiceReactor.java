package com.b6w7.eece411.P02.nio;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.HandlerThread;
import com.b6w7.eece411.P02.multithreaded.JoinThread;

// Code for Reactor pattern obtained and modified from 
// http://gee.cs.oswego.edu/dl/cpjslides/nio.pdf

/**
 * A node in a Distributed Hash Table
 */
public class ServiceReactor implements Runnable, JoinThread {
	private static final int MAX_ACTIVE_TCP_CONNECTIONS = 512;
	/** ... 
	private final Map<ByteArrayWrapper, byte[]> dht = new HashMap<ByteArrayWrapper, byte[]>((int)(40000*1.2));
	... */
	private final ConsistentHashing<ByteArrayWrapper, byte[]> dht;

	private final HandlerThread dbHandler = new HandlerThread();

	public final int serverPort;
	private boolean keepRunning = true;
	private Integer threadSem = new Integer(MAX_ACTIVE_TCP_CONNECTIONS);

	private static boolean IS_VERBOSE = Command.IS_VERBOSE;	private static boolean IS_SHORT = Command.IS_SHORT;

	private final ConcurrentLinkedQueue<SocketRegisterData> registrations 
	= new ConcurrentLinkedQueue<SocketRegisterData>();

	final Selector selector;
	final ServerSocketChannel serverSocket;
	final InetAddress inetAddress;
	final MembershipProtocol membership;

	public ServiceReactor(int servPort, String[] nodesFromFile) throws IOException, NoSuchAlgorithmException {
		if (nodesFromFile != null) 
			nodes = nodesFromFile;
		
		this.dht = new ConsistentHashing<ByteArrayWrapper, byte[]>(nodes);
		serverPort = servPort;
		InetAddress tempInetAddress;
		try {
			tempInetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// if DNS lookup fails ...
			tempInetAddress = InetAddress.getByName("localhost");
		}
		inetAddress = tempInetAddress;
		
		selector = Selector.open();
		serverSocket = ServerSocketChannel.open();

		System.out.println("Java version is " + System.getProperty("java.version"));
//		if (System.getProperty("java.version").startsWith("1.7"))
			//serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			serverSocket.socket().setReuseAddress(true);
		
		String localhost = InetAddress.getLocalHost().getHostName();//.getCanonicalHostName();
		int position = dht.getNodePosition(localhost+":"+serverPort);

		membership = new MembershipProtocol(position, dht.getSizeAllNodes());

		dht.setMembership(membership);
		
		if(IS_VERBOSE) System.out.println(" &&& ServiceReactor() [localhost, position, totalnodes]: ["+localhost+","+position+","+ dht.getSizeAllNodes()+"]");
		if (position <0)
			if(IS_VERBOSE) System.out.println(" &&& Handler() position is negative! " + position);

		serverSocket.socket().bind(new InetSocketAddress(serverPort));
		serverSocket.configureBlocking(false);
		SelectionKey sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		sk.attach(new Acceptor(this));
	}

	// code for ExecutorService obtained and modified from 
	// http://www.javacodegeeks.com/2013/01/java-thread-pool-example-using-executors-and-threadpoolexecutor.html

	@Override
	public void run() {
		System.out.println("Server listening on port " + serverPort + " with address: "+inetAddress);

		// start handler thread
		dbHandler.start();

		// we are listening, so now allocated a ThreadPool to handle new sockets connections
//		executor = Executors.newFixedThreadPool(MAX_ACTIVE_TCP_CONNECTIONS);

		while (keepRunning) {
			try {
				// block until a key has non-blocking operation available
				selector.select();
				
				// iterate and dispatch each key in set 
				Set<SelectionKey> keySet = selector.selectedKeys();
				for (SelectionKey key : keySet)
					dispatch(key);
				
				// clear the key set in preparation for next invocation of .select()
				keySet.clear(); 
				
				SocketRegisterData data;
				while (registrations.size() > 0) {
					if(!IS_SHORT) System.out.println("--- ServiceReactor()::run() connecting to remote host");
					data = registrations.poll();
					data.key = data.sc.register(selector, data.ops, data.cmd);

					data.sc.connect(data.addr);
					if(IS_SHORT) System.out.println("--- checkLocal() woke up selector");
				}
				
			} catch (IOException ex) { /* ... */ }
			finally{
				//TODO: Show contents of DHT when complete.
				if (false)
					sampleDHT();
			}
		}
		
		System.out.println("Waiting for handler thread to stop");

		if (null != dbHandler) {
			dbHandler.kill();
			do {
				try {
					dbHandler.join();
				} catch (InterruptedException e) { /* do nothing */ }
			} while (dbHandler.isAlive());
		}

		System.out.println("All threads completed");
	}

	class Acceptor implements Runnable { // inner
		private final JoinThread parent;
		Acceptor(JoinThread parent) {
			this.parent = parent;
		}
		public void run() {
			try {
				if(IS_SHORT) System.out.println("*** Acceptor::Accepting Connection");
				
				SocketChannel c = serverSocket.accept();
				if (c != null)
					new Handler(selector, c, dbHandler, dht, registrations, serverPort, membership, parent);
				
			} catch(IOException ex) { /* ... */ }
		} 
	}

	void dispatch(SelectionKey k) {
		Runnable r = (Runnable)(k.attachment());
		if (r != null) 
			r.run();
	} 

	public static void main(String[] args) {
		if (args.length > 2) {
			printUsage();
			return;
		}

		int servPort = Integer.parseInt(args[0]);

		String participatingNodes[] = null;

		if (args.length == 2) {
			String filename = args[1];
			try {
				participatingNodes = populateNodeList(filename);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				System.out.println("Error in reading: "+filename);
				//return;
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("Error in reading: "+filename);
				//return;
			}
		}
		
		ServiceReactor service;
		try {
			service = new ServiceReactor(servPort, participatingNodes);
			new Thread(service).start();
		} catch (IOException e) {
			System.out.println("Could not start service. " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Could not start hashing service. " + e.getMessage());
		}
	}

	private static void printUsage() {
		System.out.println("USAGE:\n"
				+ " java -cp"
				+ " <file.jar>"
				+ " <server port>"
				+ " <filename>");
		System.out.println("EXAMPLE:\n"
				+ " java -cp"
				+ " P03.jar"
				+ " 11111"
				+ " nodes.txt");
	}

	@Override
	public void announceDeath() {
		final Timer t = new Timer();

		t.schedule(new TimerTask() {
			
			@Override
			public void run() {
				System.out.println("     announceDeath() keepRunning = false");
				keepRunning = false;
				selector.wakeup();
				t.cancel();
			}
		}, 2000);
	}
	
	private void sampleDHT(){
		try{
			System.out.println("Getting nodes...");
			
			Map<ByteArrayWrapper, byte[]> mn = this.dht.getCircle();
			Iterator<Entry<ByteArrayWrapper, byte[]>> is = mn.entrySet().iterator();
			
			//Testing: Retrieval of nodes in the map.
			System.out.println("Got nodes... "+mn.size());
			synchronized(mn){
				while(is.hasNext()){
					Entry<ByteArrayWrapper, byte[]> e = is.next();
					System.out.println("(Key,Value): "+e.getKey() +" "+ new String(e.getValue()) );
				}
			}
		}catch(ConcurrentModificationException cme){
			// synchronized(){......} should occur before using the iterator for ConsistentHashing.java
			cme.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static String[] populateNodeList(String filename) throws FileNotFoundException, IOException{
	    String token1;
	    BufferedReader inFile
	    = new BufferedReader(new FileReader(filename));

	    List<String> nodesList = new LinkedList<String>();

	    while ((token1 = inFile.readLine()) != null) {
	    	nodesList.add(token1);
	    }
	    inFile.close();


	    String[] nodes = nodesList.toArray(new String[nodesList.size()]);
	    return nodes;

	}
//	private String[] nodes = {"planetlab2.cs.ubc.ca",
//			"cs-planetlab4.cs.surrey.sfu.ca",
//			"planetlab03.cs.washington.edu",
//			"pl1.csl.utoronto.ca",
//			"pl2.rcc.uottawa.ca",
//			"Furry.local",	//When adding/removing nodes, must change the value of NodeCommands.LEN_TIMESTAMP_BYTES accordingly.
//			"Knock3-Tablet",
//			InetAddress.getLocalHost().getHostName()};
	public static String[] nodes = 
//			{"dhcp-128-189-74-168.ubcsecure.wireless.ubc.ca:11111", 
//						"dhcp-128-189-74-168.ubcsecure.wireless.ubc.ca:11112"};
			{"Knock3-Tablet:11111", "Knock3-Tablet:11112"};
}

