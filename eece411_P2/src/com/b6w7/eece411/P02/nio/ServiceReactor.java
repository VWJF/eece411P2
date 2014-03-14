package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.HandlerThread;
import com.b6w7.eece411.P02.multithreaded.JoinThread;

// Code for Reactor pattern obtained and modified from 
// http://gee.cs.oswego.edu/dl/cpjslides/nio.pdf

/**
 * A node in a Distributed Hash Table
 */
public class ServiceReactor implements Runnable, JoinThread {
	private static final int MAX_ACTIVE_TCP_CONNECTIONS = 512;
	private final Map<ByteArrayWrapper, byte[]> dht = new HashMap<ByteArrayWrapper, byte[]>((int)(40000*1.2));
	private final HandlerThread dbHandler = new HandlerThread();

	private int serverPort;
	private ExecutorService executor;
	private boolean keepRunning = true;
	private Integer threadSem = new Integer(MAX_ACTIVE_TCP_CONNECTIONS);

	private static boolean IS_VERBOSE = false;

	final Selector selector;
	final ServerSocketChannel serverSocket;
	final InetAddress inetaddress;

	public ServiceReactor(int servPort) throws IOException {
		serverPort = servPort;
		inetaddress = InetAddress.getLocalHost();
		selector = Selector.open();
		serverSocket = ServerSocketChannel.open();
		serverSocket.socket().bind(new InetSocketAddress(serverPort));
		serverSocket.configureBlocking(false);
		SelectionKey sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		sk.attach(new Acceptor()); 
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
				
			} catch (IOException ex) { /* ... */ }
		}

//		System.out.println("Waiting worker threads to stop");
//		executor.shutdown();
//		while (!executor.isTerminated()) { 		}

		System.out.println("Waiting for handler thread to stop");
		if (null != dbHandler) {
			dbHandler.keepRunning = false;
			do {
				try {
					dbHandler.join();
				} catch (InterruptedException e) { /* do nothing */ }
			} while (dbHandler.isAlive());
		}

		System.out.println("All threads completed");
	}

	class Acceptor implements Runnable { // inner
		public void run() {
			try {
				SocketChannel c = serverSocket.accept();
				if (c != null)
					new Handler(selector, c, dbHandler, dht);
				
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
		ServiceReactor service;
		try {
			service = new ServiceReactor(servPort);
			new Thread(service).start();
		} catch (IOException e) {
			System.out.println("Could not start service. " + e.getMessage());
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
}