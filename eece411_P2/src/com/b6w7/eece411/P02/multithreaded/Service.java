package com.b6w7.eece411.P02.multithreaded;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Service extends Thread implements JoinThread {

	// 50 max on planetlab
	// -1 for serverSocket itself that listens which is also a TCP connection
	// -1 for SSH connections for debugging
	// -5 for various planetlab connections occurring in background
	private static final int MAX_ACTIVE_TCP_CONNECTIONS = 50 -1 -1 -5;
	private static final int NUM_TCP_REJECTIONS = 2;
	private final Map<ByteArrayWrapper, byte[]> data = new HashMap<ByteArrayWrapper, byte[]>();
	private final HandlerThread handler = new HandlerThread();

	private int server_backlog = MAX_ACTIVE_TCP_CONNECTIONS;

	private ServerSocket serverSock;
	private int servPort;
	private ExecutorService executor;
	private boolean keepRunning = true;
	private Integer threadSem = new Integer(MAX_ACTIVE_TCP_CONNECTIONS);

	private static boolean IS_VERBOSE = false;
	
	public Service(int servPort) {
		this.servPort = servPort;
	}

	// code for ExecutorService obtained and modified from 
	// http://www.javacodegeeks.com/2013/01/java-thread-pool-example-using-executors-and-threadpoolexecutor.html

	@Override
	public void run() {
		Socket clientSocket;
		Runnable worker;
		boolean doServe;
		boolean hasExceeded;

		int numAvailableThreads;
		
		System.out.println("Server binding to port " + servPort);
		try {
			serverSock = new ServerSocket(servPort, server_backlog, InetAddress.getLocalHost());
			System.out.println("Listening for connections...");
			System.out.println("Server binding to port " + servPort + " with address: "+InetAddress.getLocalHost());
		} catch (IOException e1) {
			System.out.println("Failed to bind to port " + servPort);
			return;
		} 
		System.out.println();
		
		// start handler thread
		handler.start();

		// we are listening, so now allocated a ThreadPool to handle new sockets connections
		executor = Executors.newFixedThreadPool(MAX_ACTIVE_TCP_CONNECTIONS);

		while (keepRunning) {
			try {
				doServe = false;
				hasExceeded = false;

				// Spawn a new socket when client connects
				clientSocket = serverSock.accept();

				synchronized (threadSem) {
					// unexpected but if we hit 0 TCP connections allowed, 
					// then we simply close the socket and do not reply to client
					if (threadSem == 0) {
						hasExceeded = true;
						numAvailableThreads = threadSem;

					} else if (threadSem <= NUM_TCP_REJECTIONS) {
						// we cannot service this TCP connection, but
						// we still have a few TCP connections available, so
						// we reply that server is overloaded
						threadSem --;
						numAvailableThreads = threadSem;

					} else {
						// we have available a TCP socket to service this client
						doServe = true;
						threadSem --;
						numAvailableThreads = threadSem;
					}
				}

				if (hasExceeded) {
					if (IS_VERBOSE) System.err.println("j Exceeded maximum of " + MAX_ACTIVE_TCP_CONNECTIONS + " connections.  Closing incoming socket.");
					clientSocket.close();
					continue;
				}

				System.out.println("Handling client at " +
						clientSocket.getInetAddress().getHostAddress() + " with " + numAvailableThreads + " threads available.");

				if (doServe) {
					// Spawn a worker thread to service the client 
					worker = new WorkerThread(clientSocket, handler, data, this);
				} else {
					// Spawn a worker thread merely to reject the client 
					worker = new WorkerThread(clientSocket, this);
				}
				
				// schedule to start
				executor.execute(worker);

			} catch (IOException e) {
				System.out.println("Unknown IO Exception, closing the client socket");
			}
		}

		System.out.println("Waiting worker threads to stop");
		executor.shutdown();
		while (!executor.isTerminated()) {
		}

		System.out.println("Waiting for handler thread to stop");
		if (null != handler) {
			handler.keepRunning = false;
			do {
				try {
					handler.join();
				} catch (InterruptedException e) { /* do nothing */ }
			} while (handler.isAlive());
		}

		System.out.println("All threads completed");
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			printUsage();
			return;
		}

		int servPort = Integer.parseInt(args[0]);
		Service service = new Service(servPort);
		service.start();
	}

	private static void printUsage() {
		System.out.println("USAGE:\n"
				+ " java -cp"
				+ " <file.jar>"
				+ " <server port>");
		System.out.println("EXAMPLE:\n"
				+ " java -cp"
				+ " P02.jar"
				+ " 55699");
	}

	@Override
	public void announceDeath() {
		// announce the release of a TCP resource
		synchronized (threadSem) {
			threadSem ++;
		}
	}
}
