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
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);


	private final ConcurrentLinkedQueue<SocketRegisterData> registrations 
	= new ConcurrentLinkedQueue<SocketRegisterData>();

	final Selector selector;
	final ServerSocketChannel serverSocket;
	final InetAddress inetAddress;
	final MembershipProtocol membership;
	private Timer timer;
	private JoinThread self;

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

		log.info("Java version is {}", System.getProperty("java.version"));
		
//		if (System.getProperty("java.version").startsWith("1.7"))
			//serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			serverSocket.socket().setReuseAddress(true);
		
		String localhost = InetAddress.getLocalHost().getHostName();//.getCanonicalHostName();
		int position = dht.getNodePosition(localhost+":"+serverPort);

		membership = new MembershipProtocol(position, dht.getSizeAllNodes());

		dht.setMembership(membership);
		
		log.debug(" &&& ServiceReactor() [localhost, position, totalnodes]: [{}, {}, {}]", localhost, position, dht.getSizeAllNodes());
		if (position <0)
			log.warn(" &&& Handler() position is negative {}!", position);

		serverSocket.socket().bind(new InetSocketAddress(serverPort));
		serverSocket.configureBlocking(false);
		SelectionKey sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		sk.attach(new Acceptor(this));

		self = this;
	}

	// code for ExecutorService obtained and modified from 
	// http://www.javacodegeeks.com/2013/01/java-thread-pool-example-using-executors-and-threadpoolexecutor.html

	@Override
	public void run() {
		log.info("Server listening on port {} with address {}", serverPort, inetAddress);

		timer = new Timer();
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				try {
					log.trace("ServiceReactor::Timer::run() Spawning new Handler for TSPushProcess");
					Command cmd = new Handler(selector, dbHandler, dht, registrations, serverPort, membership, self);
					dbHandler.post(cmd);
				} catch (IOException e) {
					log.debug(e.getMessage());
				}
				
			}
		}, 
		2000, 10000);
		
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
					log.trace("--- ServiceReactor()::run() connecting to remote host");
					data = registrations.poll();
					data.key = data.sc.register(selector, data.ops, data.cmd);

					data.sc.connect(data.addr);
				}
				
			} catch (IOException ex) { /* ... */ }
			finally{
				//TODO: Show contents of DHT when complete.
				if (false)
					sampleDHT();
			}
		}
		
		log.debug("Waiting for timer thread to stop");
		
		if (null != timer) 
				timer.cancel();

		log.debug("Waiting for handler thread to stop");

		if (null != dbHandler) {
			dbHandler.kill();
			do {
				try {
					dbHandler.join();
				} catch (InterruptedException e) { /* do nothing */ }
			} while (dbHandler.isAlive());
		}

		log.info("All threads completed");
		
		// This will shut it down for all JVM's running, so comment out for now
		//LogManager.shutdown();
	}

	class Acceptor implements Runnable { // inner
		private final JoinThread parent;
		Acceptor(JoinThread parent) {
			this.parent = parent;
		}
		public void run() {
			try {
				log.trace(" *** Acceptor::Accepting Connection");
				
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

	public static void main(String[] args) throws FileNotFoundException {
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
				log.error("Error in reading: {}", filename);
				//return;
			} catch (IOException e1) {
				e1.printStackTrace();
				log.error("Error in reading: {}", filename);
				//return;
			}
		}
		
		ServiceReactor service;
		try {
			
			if (servPort != 0) {
				log.info("Starting one service on local host");
				// If server port was specified (non-zero) then we start one server
				service = new ServiceReactor(servPort, participatingNodes);
				new Thread(service).start();
				
			} else {
				log.info("Starting multiple services on local host");
				// If server port was special case (zero) then we start every participating
				// node on localhost with this one launch
				for (String node : participatingNodes) {
					service = new ServiceReactor(Integer.valueOf(node.split(":")[1]), participatingNodes);
					new Thread(service).start();
				}
			}
		} catch (IOException e) {
			log.error("Could not start service. {}", e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			log.error("Could not start hashing service. {}", e.getMessage());
		}
	}
	
	private static void printUsage() {
		log.info("USAGE:\n"
				+ " java -cp"
				+ " <file.jar>"
				+ " <server port>"
				+ " <filename>");
		log.info("EXAMPLE:\n"
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
				log.info("Signalling to Service to shutdown.");
				keepRunning = false;
				selector.wakeup();
				t.cancel();
			}
		}, 2000);
	}
	
	private void sampleDHT(){
		try{
			log.debug("Getting nodes...");
			
			Map<ByteArrayWrapper, byte[]> mn = this.dht.getCircle();
			Iterator<Entry<ByteArrayWrapper, byte[]>> is = mn.entrySet().iterator();
			
			//Testing: Retrieval of nodes in the map.
			log.debug("Got nodes... {}", mn.size());
			synchronized(mn){
				while(is.hasNext()){
					Entry<ByteArrayWrapper, byte[]> e = is.next();
					log.debug("(Key,Value): {} {}", e.getKey(), new String(e.getValue()) );
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
	
	/**
	 * Creates a String[] for each line in the given file.
	 * The file should not have an empty lines
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static String[] populateNodeList(String filename) throws FileNotFoundException, IOException{
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

	public static String[] nodes = 
		{
		"Knock3-Tablet:11111", "Knock3-Tablet:11112", "Knock3-Tablet:11113", "Knock3-Tablet:11114",
		"Knock3-Tablet:11115", "Knock3-Tablet:11116", "Knock3-Tablet:11117", "Knock3-Tablet:11118",
		"Knock3-Tablet:11119", "Knock3-Tablet:11120", "Knock3-Tablet:11121", "Knock3-Tablet:11122"
		};
}

