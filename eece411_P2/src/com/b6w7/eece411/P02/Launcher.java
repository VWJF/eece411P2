package com.b6w7.eece411.P02;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

public class Launcher {
	
	private static final int REQSIZE = 1+32+1024;   // Size of receive buffer
	private static final int RESSIZE = 1+1024;   // Size of response buffer

	private static final int NUM_CLIENTS = 15;
	
	//private static ArrayList<Socket> connected_clients;
	private static ArrayBlockingQueue<ClientInterface> connected_clients;
	
	public static void main(String[] args) throws IOException {
		
		//connected_clients = new ArrayList<Socket>(NUM_CLIENTS);
		connected_clients = new ArrayBlockingQueue<ClientInterface>(NUM_CLIENTS);
		
		int servPort = Integer.parseInt(args[0]);

		// Create a server socket to accept client connection requests
		ServerSocket servSock = new ServerSocket(servPort);

		int recvMsgSize = 0;   // Size of received message
		//byte[] byteBuffer = new byte[REQSIZE];  // Receive buffer
		char[] charBufferIn = new char[REQSIZE];  // Receive buffer for BufferedReader inFromClient
		byte[] byteBufferOut = new byte[RESSIZE];  // Response buffer

		for (;;) { // Run forever, accepting and servicing connections
		
		// Thread 1 accepting connections			

			Socket clientSock = servSock.accept();     // Get client connection, Blocking call
		
			System.out.println("Handling client at " +
					clientSock.getInetAddress().getHostAddress());

			//InputStream in = clientSock.getInputStream();
			//OutputStream out = clientSock.getOutputStream();
			DataOutputStream outToClient = 
					new DataOutputStream(clientSock.getOutputStream());
			BufferedReader inFromClient = 
					new BufferedReader(new InputStreamReader(clientSock.getInputStream()), REQSIZE );
				
			try {
				// Receive REQSIZE number of bytes or 
				// until client closes connection, indicated by -1 return 
				while ((recvMsgSize = inFromClient.read(charBufferIn, 0, REQSIZE)) != -1)
					;
					
			} catch (IOException e1) {
				e1.printStackTrace();
			}			
			
			//Parse and Extract relevant data
			ByteBuffer dataRead = ByteBuffer.wrap(	new String(charBufferIn).getBytes() );
			byte cmd = dataRead.get();		//Read 1 byte
			byte[] key = new byte[32];		dataRead.get(key, 1, key.length);
			byte[] value = new byte[1024];	dataRead.get(value, 33, value.length);
			
			ClientInterface ci = new ClientInterface(clientSock, cmd, 
												ByteBuffer.wrap(key), 
												ByteBuffer.wrap(value) );
			
			// Add client socket and received command to the queue of connected clients.
			if( !connected_clients.offer(ci) ){
				System.out.println("Max connected clients.\n");
				clientSock.close();
				continue;
			}
			
		// Thread 2 executing commands			
			//Execute received command.
			
			// TODO: Currently ClientInterface.getReply() will execute the command before generating a reply.
			// getReply() is called from the Thread sending replies.

			
		// Thread 3 Sending replies commands			
			
			//Obtain a connected client and reply to the client with its response.
			ClientInterface clientToReply =	connected_clients.poll();

			// Send reply to client
			if(clientToReply != null){
				try {
					byteBufferOut= clientToReply.getReply().array();
					outToClient.write(byteBufferOut, 0, recvMsgSize);

				} catch (IOException e1) {
					// TODO:
					e1.printStackTrace();
				}
				clientToReply.getSocket().close(); // Close the socket.  We are done with this client!
			}
			
			
			//clientSock.close();  // Close the socket.  We are done with this client!
		}
	}
}
