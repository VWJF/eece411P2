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
		executor = Executors.newFixedThreadPool(31);

		while (keepRunning) {
			try {
				serverSock = new ServerSocket(servPort);  // Start listening for connections

				clientSocket = serverSock.accept();       // Get client connection, Blocking call

				System.out.println("Handling client at " +
						clientSocket.getInetAddress().getHostAddress());

				worker = new WorkerThread(clientSocket, handler, data);

				executor.execute(worker);
				
			} catch (IOException e) {
				System.out.println("Unknown IO Exception");
			}
		}
		
		handler.keepRunning = false;
		
		executor.shutdown();

		while (!executor.isTerminated()) {
		}

		System.out.println("Finished all threads");
	}

	public static void main(String[] args) {
		int servPort = Integer.parseInt(args[0]);
		Service service = new Service(servPort);
		service.start();
	}
}
