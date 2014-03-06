package com.b6w7.eece411.P02.multithreaded;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Service extends Thread {

	private final Map<String, String> data = new HashMap<String, String>();
	private final HandlerThread handler = new HandlerThread();

	private ServerSocket serverSock;
	private int servPort;
	private ExecutorService executor;
	private boolean keepRunning = true;

	public Service(int servPort) {
		this.servPort = servPort;
	}

	// code for ExecutorService obtained and modified from 
	// http://www.javacodegeeks.com/2013/01/java-thread-pool-example-using-executors-and-threadpoolexecutor.html

	@Override
	public void run() {
		Socket clientSocket;
		Runnable worker;

		System.out.println("Server binding to port " + servPort);
		try {
			serverSock = new ServerSocket(servPort);
			System.out.println("Listening for connections...");
		} catch (IOException e1) {
			System.out.println("Failed to bind to port " + servPort);
			return;
		} 

		// we are listening, so now allocated a ThreadPool to handle new sockets connections
		executor = Executors.newFixedThreadPool(30);

		System.out.println("Listening for connections...");
		while (keepRunning) {
			try {

				// Spawn a new socket when client connects
				clientSocket = serverSock.accept();
				System.out.println("Handling client at " +
						clientSocket.getInetAddress().getHostAddress());

				// Spawn a worker thread for the client socket and schedule to start
				worker = new WorkerThread(clientSocket, handler, data);
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

}
