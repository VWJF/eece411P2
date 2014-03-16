package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.HandlerThread;
import com.b6w7.eece411.P02.multithreaded.JoinThread;
import com.b6w7.eece411.P02.nio.ConsistentHashing;

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
	private final ConsistentHashing dht;

	private final HandlerThread dbHandler = new HandlerThread();

	private int serverPort;
	private ExecutorService executor;
	private boolean keepRunning = true;
	private Integer threadSem = new Integer(MAX_ACTIVE_TCP_CONNECTIONS);

	private static boolean IS_VERBOSE = Command.IS_VERBOSE;	private static boolean IS_SHORT = Command.IS_SHORT;

	private final ConcurrentLinkedQueue<SocketRegisterData> registrations 
	= new ConcurrentLinkedQueue<SocketRegisterData>();

	final Selector selector;
	final ServerSocketChannel serverSocket;
	final InetAddress inetaddress;
	// debugging flag
	public final boolean USE_REMOTE;

	public ServiceReactor(int servPort) throws IOException, NoSuchAlgorithmException {
		this.dht = new ConsistentHashing(nodes);
		serverPort = servPort;
		inetaddress = InetAddress.getLocalHost();
		selector = Selector.open();
		serverSocket = ServerSocketChannel.open();

		System.out.println("Java version is " + System.getProperty("java.version"));
//		if (System.getProperty("java.version").startsWith("1.7"))
//			serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		
		serverSocket.socket().bind(new InetSocketAddress(serverPort));
		serverSocket.configureBlocking(false);
		SelectionKey sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		sk.attach(new Acceptor());
		
		// TODO remove hack for debugging
		if (servPort == 11111) {
			System.out.println("Using Remote");
			USE_REMOTE = true; 

		} else {
			System.out.println("Using Local");
			USE_REMOTE = false;
			servPort = 11112; 
		}

	}

	// code for ExecutorService obtained and modified from 
	// http://www.javacodegeeks.com/2013/01/java-thread-pool-example-using-executors-and-threadpoolexecutor.html

	@Override
	public void run() {
		System.out.println("Server listening on port " + serverPort + " with address: "+inetaddress);

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
					if(IS_SHORT) System.out.println("--- Found registration to connect to 11112");
					data = registrations.poll();
					data.key = data.sc.register(selector, data.ops, data.cmd);

					data.sc.connect(new InetSocketAddress(11112));
					if(IS_SHORT) System.out.println("--- checkLocal() woke up selector");
				}
				
			} catch (IOException ex) { /* ... */ }
			finally{
				//TODO: Show contents of DHT when complete.
				if (false)
					sampleDHT();
			}
		}
		
		//TODO: Not Reached ......??	

		System.out.println("Waiting for handler thread to stop");

//		System.out.println("Waiting worker threads to stop");
//		executor.shutdown();
//		while (!executor.isTerminated()) { 		}

		if (null != dbHandler) {
			dbHandler.keepRunning = false;
			do {
				try {
					dbHandler.join();
				} catch (InterruptedException e) { /* do nothing */ }
			} while (dbHandler.isAlive());
		}
		//TODO: Not Reached ......??
		System.out.println("All threads completed");
	}

	class Acceptor implements Runnable { // inner
		public void run() {
			try {
				if(IS_SHORT) System.out.println("*** Acceptor::Accepting Connection");
				
				SocketChannel c = serverSocket.accept();
				if (c != null)
					new Handler(selector, c, dbHandler, dht, registrations, USE_REMOTE);
				
			} catch(IOException ex) { /* ... */ }
		} 
	}

	void dispatch(SelectionKey k) {
		Runnable r = (Runnable)(k.attachment());
		if (r != null) 
			r.run();
	} 

	public static void main(String[] args) {
		if (args.length != 1) {
			printUsage();
			return;
		}

		int servPort = Integer.parseInt(args[0]);

		//servPort = 11112;
		
		ServiceReactor service;
		try {
			service = new ServiceReactor(servPort);
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
				+ " <server port>");
		System.out.println("EXAMPLE:\n"
				+ " java -cp"
				+ " P03.jar"
				+ " 11111");
	}

	@Override
	public void announceDeath() {
		// announce the release of a TCP resource
		synchronized (threadSem) {
			threadSem ++;
		}
	}
	
	private void sampleDHT(){
		try{
			System.out.println("Getting nodes...");
			
			SortedMap<ByteArrayWrapper, byte[]> mn = this.dht.getCircle();
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
	private String[] nodes = {"planetlab2.cs.ubc.ca",
			"cs-planetlab4.cs.surrey.sfu.ca",
			"planetlab03.cs.washington.edu",
			"pl1.csl.utoronto.ca",
			"pl2.rcc.uottawa.ca"};
}
