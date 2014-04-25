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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.HandlerThread;
import com.b6w7.eece411.P02.multithreaded.JoinThread;

// Code for Reactor pattern obtained and modified from 
// http://gee.cs.oswego.edu/dl/cpjslides/nio.pdf

/**
 * A node in a Distributed Hash Table.
 */
public class ServiceReactor implements Runnable, JoinThread, Gossip {
	// Parameters for tuning performance
	/**	true to periodically gossip with all offline nodes with period {@link #PERIOD_GOSSIP_OFFLINE_MS} */
	private final boolean ENABLE_GOSSIP_OFFLINE = true; 
	/**	true to periodically gossip with one online node with period {@link #PERIOD_GOSSIP_RANDOM_MS}*/
	private final boolean ENABLE_GOSSIP_RANDOM  = true; 
	/**	true to periodically gossip with one replica node with period {@link #PERIOD_GOSSIP_REPLICA_MS}*/
	private final boolean ENABLE_GOSSIP_REPLICA = true; 

	/**	The time interval after gossiping with all offline nodes before gossiping again.
	 * A smaller number slows performance with larger number of offline nodes */
	private final long PERIOD_GOSSIP_OFFLINE_MS = 10000;
	/**	The time interval after gossiping with one online node before gossiping again.
	 * A smaller number propagates offline information faster */
	private final long PERIOD_GOSSIP_RANDOM_MS  = 3500;
	/**	The time interval after gossiping with one online node before gossiping again.
	 * A smaller number propagates offline information faster */
	private final long PERIOD_GOSSIP_REPLICA_MS  = 300;

	/**	The upper bound on TCP timeout with another node.  All TCP connections will timeout at this ceiling. */
	private final long TIME_MAX_TIMEOUT_MS = 750;
	/**	The lower bound on TCP timeout with another node.  All TCP connections will not timeout sooner than this floor. */
	private final long TIME_MIN_TIMEOUT_MS = 350;

	/** Logging interface. */
	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);

	/** A list of out bound TCP connections to be initiated. */
	private final ConcurrentLinkedQueue<SocketRegisterData> registrations 
	= new ConcurrentLinkedQueue<SocketRegisterData>();
	/** A distributed hash table.  This table stores nodes as well as key-value pairs. */
	private final ConsistentHashing<ByteArrayWrapper, byte[]> dht;

	/** The sole thread that handles interactions with the entire local model. */
	private final HandlerThread dbHandler = new HandlerThread();
	/** The sole thread that handles locally-saved key-value pairs by spawning replicas to remote nodes. */
	private final ReplicaThread replicaHandler = new ReplicaThread(dbHandler);

	private final ServiceReactor self = this;
	private final Selector selector;
	private final ServerSocketChannel serverSocket;
	private final String localhost;
	private final InetAddress inetAddress;
	private final int serverPort;
	private final MembershipProtocol membership;
	private final int positionInRing;
	
	private final Timer timer = new Timer();
	private TimerTask taskGossipRandom = null;
	private TimerTask taskGossipOffline = null;
	private TimerTask taskGossipReplica = null;

	private boolean keepRunning = true;
	
	public ServiceReactor(int servPort, String[] nodesFromFile) throws IOException, NoSuchAlgorithmException {
		// Receive list of nodes in the node ring
		if (nodesFromFile != null) 
			nodes = nodesFromFile;

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String n : nodes){
			if (i++ % 5 == 0)
				sb.append("\n");
			sb.append(n+"\t");
		}
		
		log.info("List of nodes (size= {}): {}", nodes.length, sb.toString());
		
		// Find localhost
		InetAddress tempInetAddress;
		try {
			tempInetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// if DNS lookup fails ...
			tempInetAddress = InetAddress.getByName("localhost");
		}
		inetAddress = tempInetAddress;
		serverPort = servPort;

		// Configure DHT and Membership
		dht = new ConsistentHashing<ByteArrayWrapper, byte[]>(nodes);
		localhost = inetAddress.getHostName();
		positionInRing = dht.getNodePosition(localhost+":"+serverPort);
		dht.setLocalNode(localhost+":"+serverPort);
		membership = new MembershipProtocol(positionInRing, dht.getSizeAllNodes(), TIME_MAX_TIMEOUT_MS, TIME_MIN_TIMEOUT_MS);
		dht.setMembership(membership);
		
		//membership.addObserver(dht);
//		membership.setMap(dht);

		// start the server socket in non-blocking mode and NIO selector and 
		selector = Selector.open();
		serverSocket = ServerSocketChannel.open();
		serverSocket.socket().setReuseAddress(true);
		serverSocket.socket().bind(new InetSocketAddress(serverPort));
		serverSocket.configureBlocking(false);
		SelectionKey sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		sk.attach(new Acceptor(this));
	}

	@Override
	public void run() {
		// Show some status information
		log.info("Java version is {}", System.getProperty("java.version"));
		log.info("Localhost is {}, position in ring is {}, total nodes is {}", localhost, positionInRing, dht.getSizeAllNodes());
		if (positionInRing < 0)
			log.warn("This node's position in the node ring is negative {}!", positionInRing);
		log.info("Server listening on port {} with address {}", serverPort, inetAddress);

		// schedule an immediate gossiping
		if (ENABLE_GOSSIP_OFFLINE) armGossipOffline(0);
		if (ENABLE_GOSSIP_RANDOM)  armGossipOnline(0);
		if (ENABLE_GOSSIP_REPLICA) armGossipReplica(0);

		// start handler thread and replica thread
		dbHandler.start();
		replicaHandler.start();

		while (keepRunning) {
			try {
				
				// block until a key has non-blocking operation available
				selector.select(100);

				// iterate and dispatch each key in set 
				Set<SelectionKey> keySet = selector.selectedKeys();
				for (SelectionKey key : keySet)
					dispatch(key);

				for (SelectionKey key: selector.keys()) {
					if (key.isValid() && key.interestOps() == SelectionKey.OP_READ) {
						Handler handler = ((Handler)key.attachment());
						long now = new Date().getTime(); 
						if (now - handler.timeStart - handler.timeTimeout  > 0) {
							// too much time has passed whilst waiting for reply from owner, so enforce timeout
							if ( handler.state == Handler.State.RECV_OWNER ) {
								log.trace(" %%% OP_READ timeout {} {}", now - handler.timeStart - handler.timeTimeout, handler);
								handler.retryAtStateCheckingLocal(new TimeoutException());
							}
						}
					}
				}

				// clear the key set in preparation for next invocation of .select()
				keySet.clear(); 

				SocketRegisterData data;
				while (registrations.size() > 0) {
					log.trace("--- ServiceReactor()::run() connecting to remote host");
					data = registrations.poll();
					data.key = data.sc.register(selector, data.ops, data.cmd);

					data.sc.socket().setSoTimeout(2000);
					data.sc.connect(data.addr);
				}

			} catch (IOException ex) { 
				log.error(ex.getMessage());
			}
		}
		
		try {
			selector.close();
		} catch (IOException e1) {}
		
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
		if (null != replicaHandler) {
			replicaHandler.kill();
			do {
				try {
					replicaHandler.join();
				} catch (InterruptedException e) { /* do nothing */ }
			} while (replicaHandler.isAlive());
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
					new Handler(selector, c, dbHandler, replicaHandler, dht, registrations, serverPort, membership, parent);
				
			} catch(IOException ex) { /* ... */ }
		} 
	}

	void dispatch(SelectionKey k) {
		Runnable r = (Runnable)(k.attachment());
		if (r != null) 
			r.run();
	} 
	
	@Override
	public void armGossipReplica() {
		armGossipReplica(PERIOD_GOSSIP_REPLICA_MS);
	}

	private void armGossipReplica(long delay) {
		
		taskGossipReplica = new TimerTask() {

			@Override
			public void run() {
				try {
					log.trace("ServiceReactor::Timer::run() Spawning new Handler for TSPushReplicaProcess");
					Command cmd = new Handler(selector, dbHandler, dht, registrations, serverPort, membership, (JoinThread)self, (Gossip)self, 1);
					dbHandler.post(cmd);
				} catch (IOException e) {
					log.debug(e.getMessage());
				}

			}
		};
		
		if (timer != null && keepRunning)
			timer.schedule(taskGossipReplica, delay );
	}

	@Override
	public void armGossipOffline() {
		armGossipOffline(PERIOD_GOSSIP_OFFLINE_MS);
	}
	
	private void armGossipOffline(long delay) {
		
		taskGossipOffline = new TimerTask() {

			@Override
			public void run() {
				try {
					log.trace("ServiceReactor::Timer::run() Spawning new Handler for TSPushProcess");
					Command cmd = new Handler(selector, dbHandler, dht, registrations, serverPort, membership, (JoinThread)self, (Gossip)self, true);
					dbHandler.post(cmd);
				} catch (IOException e) {
					log.debug(e.getMessage());
				}

			}
		};
		
		if (timer != null && keepRunning)
			timer.schedule(taskGossipOffline, delay );
	}

	@Override
	public void armGossipOnline() {
		armGossipOnline(PERIOD_GOSSIP_RANDOM_MS);
	}
	
	private void armGossipOnline(long delay) {
		taskGossipRandom = new TimerTask() {

			@Override
			public void run() {
				try {
					log.trace("ServiceReactor::Timer::run() Spawning new Handler for TSPushOfflineProcess");
					Command cmd = new Handler(selector, dbHandler, dht, registrations, serverPort, membership, (JoinThread)self, (Gossip)self, false);
					dbHandler.post(cmd);
				} catch (IOException e) {
					log.debug(e.getMessage());
				}

			}
		};

		if (timer != null && keepRunning)
			timer.schedule(taskGossipRandom, delay);
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
				//participatingNodes = populateNodeList2(filename);

			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				log.error("Error in reading: {}", filename);

			} catch (IOException e1) {
				e1.printStackTrace();
				log.error("Error in reading: {}", filename);
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
		}, 0);
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
	
	public static String[] populateNodeList2(String filename) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(filename));

	    List<String> nodesList = new LinkedList<String>();

	    while (in.ready()) {
	    	String s = in.readLine();
	    	if (s != null) {
	    		String[] host = s.split(":");
	    		if (host.length == 2)
	    			nodesList.add(s);
	    		else
	    			log.warn("Reading from {}: skipping entry '{}'", filename, s);
	    	}
		}
		in.close();
		
		String[] ret = new String[nodesList.size()];
		for (int i = 0; i < nodesList.size(); i++)
			ret[i] = nodesList.get(i);
		
		return ret;
	}

	public static String[] nodes = 
		{
		"Knock3-Tablet:11111", "Knock3-Tablet:11112", "Knock3-Tablet:11113", "Knock3-Tablet:11114",
		"Knock3-Tablet:11115", "Knock3-Tablet:11116", "Knock3-Tablet:11117", "Knock3-Tablet:11118",
		"Knock3-Tablet:11119", "Knock3-Tablet:11120", "Knock3-Tablet:11121", "Knock3-Tablet:11122"
		};
}

