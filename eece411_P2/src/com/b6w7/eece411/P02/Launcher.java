package com.b6w7.eece411.P02;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

public class Launcher {
	
	private static final int REQSIZE = 1+32+1024;   // Size of receive buffer
	private static final int RESSIZE = 1+1024;   // Size of response buffer

	private static final int NUM_CLIENTS = 15;
	
	//private static ArrayList<Socket> connected_clients;
	private static ArrayBlockingQueue<Element<Socket,TestData>> connected_clients;
	
	public static void main(String[] args) throws IOException {
		
		//connected_clients = new ArrayList<Socket>(NUM_CLIENTS);
		connected_clients = new ArrayBlockingQueue<Element<Socket,TestData>>(NUM_CLIENTS);
		
		int servPort = Integer.parseInt(args[0]);

		// Create a server socket to accept client connection requests
		ServerSocket servSock = new ServerSocket(servPort);

		int recvMsgSize;   // Size of received message
		byte[] byteBuffer = new byte[REQSIZE];  // Receive buffer
		char[] charBufferIn = new char[REQSIZE];  // Receive buffer
		byte[] byteBufferOut = new byte[RESSIZE];  // Response buffer

		for (;;) { // Run forever, accepting and servicing connections
			Socket clntSock = servSock.accept();     // Get client connection
			
	
			
			System.out.println("Handling client at " +
					clntSock.getInetAddress().getHostAddress());

			InputStream in = clntSock.getInputStream();
			OutputStream out = clntSock.getOutputStream();

			DataOutputStream outToClient = 
					new DataOutputStream(clntSock.getOutputStream());
			BufferedReader inFromClient = 
					new BufferedReader(new InputStreamReader(clntSock.getInputStream()), REQSIZE );
			
			
			// Receive until client closes connection, indicated by -1 return
			while ((recvMsgSize = inFromClient.read(charBufferIn, 0, REQSIZE)) != -1)
				outToClient.write(byteBufferOut, 0, recvMsgSize);
			
			TestData sample1 = null;
			try {
				connected_clients.add(new Element<Socket,TestData>(clntSock,sample1) );
			} catch (IllegalStateException e) {
				e.printStackTrace();
				System.out.println("Max connected clients.\n"+e.getLocalizedMessage());
			}
			
			clntSock.close();  // Close the socket.  We are done with this client!
		}
	}
}
