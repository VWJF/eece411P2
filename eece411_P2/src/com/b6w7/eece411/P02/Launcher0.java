package com.b6w7.eece411.P02;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.DataFormatException;

import com.b6w7.eece411.P02.NodeCommands.Reply;
import com.b6w7.eece411.P02.NodeCommands.Request;

public class Launcher0 {

	// number of bytes in protocol field
	private static final int CMDSIZE = 1;		
	private static final int KEYSIZE = 32;
	private static final int VALUESIZE = 1024;
	
	// Size of protocol buffers
	private static final int REQSIZE = CMDSIZE+KEYSIZE+VALUESIZE;  // request buffer
	private static final int RESSIZE = CMDSIZE+VALUESIZE;   //response buffer

	private static final int NUM_CLIENTS = 15; 	//Max number or open connections to planetlab = 30
	private static final int TIMEOUT = 10;		//Server Socket timeout Seconds

	
	//private static ArrayList<Socket> connected_clients;
	private static ArrayBlockingQueue<ClientInterface> connected_clients;
	
	public static void main(String[] args) throws IOException {
		
		connected_clients = new ArrayBlockingQueue<ClientInterface>(NUM_CLIENTS);
		
		int servPort = Integer.parseInt(args[0]);

		Socket clientSock = null;
		
		int recvMsgSize = 0;   // Size of received message
		int totalBytesReceived = 0;   // Size of received message

		//byte[] byteBuffer = new byte[REQSIZE];  // Receive buffer
		char[] charBufferIn = new char[REQSIZE];  // Receive buffer for BufferedReader inFromClient requires a char[]
		byte[] byteBufferOut = new byte[RESSIZE]; // Response buffer
		
		try{
		
		// Create a server socket to accept client connection requests
		ServerSocket serverSock = new ServerSocket(servPort);
		serverSock.setSoTimeout(TIMEOUT);
		serverSock.setReceiveBufferSize(REQSIZE);
		//serverSock.setPerformancePreferences(connectionTime, latency, bandwidth);
		
		System.out.println("Listening for connections...");

		for (;;) { // Run forever, accepting and servicing connections
		
		// Thread 1 accepting connections			

			clientSock = serverSock.accept();     // Get client connection, Blocking call
				
			charBufferIn = new char[REQSIZE]; byteBufferOut = new byte[RESSIZE];
			recvMsgSize = totalBytesReceived = 0;
			
			System.out.println("Handling client at " +
					clientSock.getInetAddress().getHostAddress());

			//InputStream in = clientSock.getInputStream();
			//OutputStream out = clientSock.getOutputStream();
			DataOutputStream outToClient = 
					new DataOutputStream(clientSock.getOutputStream());
			BufferedReader inFromClient = 
					new BufferedReader(new InputStreamReader(clientSock.getInputStream()), REQSIZE );
			

			ClientInterface ci = new ClientInterface(clientSock);
			
			// Add client socket to the queue of connected clients.
			if( !connected_clients.offer(ci) ){
				System.out.println("Max connected clients.\n");
				clientSock.close();
				continue;
			}
			
			try {
				// Receive fields "Command" and "Keys" number of bytes or 
				// until client closes connection, indicated by -1 return 
				while ((recvMsgSize = inFromClient.read(charBufferIn, 0, CMDSIZE+KEYSIZE)) != -1){
					totalBytesReceived += recvMsgSize;
					if ( !checkBytes(totalBytesReceived) ){
						throw new SocketException("Incorrect bytes received CMD+KEY: " + totalBytesReceived);
					}	
				}	
			} catch (IOException e1) {
				// Error in reading from input stream.
				e1.printStackTrace();
				charBufferIn = null;
				throw new DataFormatException("Error in reading from input stream. Bytes read: " + totalBytesReceived);
			}			

			//Parse and Extract relevant data
			byte cmd;
			byte[] key;
			byte[] value;

			ByteBuffer dataRead = ByteBuffer.wrap(	new String(charBufferIn).getBytes() );
			cmd = dataRead.get();
			key = new byte[KEYSIZE];		dataRead.get(key, CMDSIZE, key.length);
			value = new byte[VALUESIZE];	dataRead.get(value, CMDSIZE+KEYSIZE, value.length);


			// Read field "Value" only if the command received is "put".
			if( cmd == (byte) Request.CMD_PUT.getCode() ){
				try {
					// Receive field "Value" number of bytes or 
					// until client closes connection, indicated by -1 return 
					while ((recvMsgSize = inFromClient.read(charBufferIn, CMDSIZE+KEYSIZE, VALUESIZE)) != -1){
						totalBytesReceived += recvMsgSize;
						if ( !checkBytes(totalBytesReceived) ){
							throw new SocketException("Incorrect bytes received VALUE: " + totalBytesReceived);
						}	

					}		
				} catch (IOException e1) {
					// Error in reading from input stream.
					e1.printStackTrace();
					charBufferIn = null;
					throw new DataFormatException("Error in reading from input stream. Bytes read: " + totalBytesReceived);
				}	
			}
			
			// set the request received to associated client.
			ci.setRequest(cmd, ByteBuffer.wrap(key), ByteBuffer.wrap(value) );
			
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
					outToClient.write(byteBufferOut, 0, byteBufferOut.length);

					System.out.println("Closing socket.");
				} catch (IOException e1) {
					// TODO:
					// Error in writing to output stream.
					e1.printStackTrace();
				}
				clientToReply.getSocket().close(); // Close the socket.  We are done with this client!
			}
			
			
			//clientSock.close();  // Close the socket.  We are done with this client!
		}
		
	} catch (DataFormatException e) {
		//Error in reading from input stream
		
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		System.out.println("Network error occurred.");
		
	} finally {
		if (null != clientSock) {
			try {
				// Close the socket. We are done with this client!
				clientSock.close();
			} catch (IOException e) {}
		} 
	}
	}
	/*
	 * given the number of bytes read,
	 * returns true if the number of bytes match the size of the fields (CMD, CMD+KEY, CMD+KEY+VALUE),
	 * 		   false otherwise
	 */
	public static boolean checkBytes(int totalBytesRead){
		boolean error = false;

		switch (totalBytesRead) {
		case CMDSIZE: // Field "Command" has been read.
			System.out.println("Received Bytes in CMD: " + totalBytesRead);

			//System.out.println("Received Cmd: " + Integer.toString((byteBuffer[0] & 0xff) + 0x100, 16).substring(1));
			System.out.println("--- State changed from CMD -> KEY");
			error = true;
			break;

		case CMDSIZE+KEYSIZE: // Field "KEY" has been read.
			System.out.println("Received Bytes in CMD+KEY: " + totalBytesRead);

			//System.out.println("Received Key: " + totalBytesRead);
			System.out.println("--- State changed from KEY -> VALUE");
			error = true;
		break;

		case CMDSIZE+KEYSIZE+VALUESIZE: // Field "VALUE" has been read.

			System.out.println("Received Bytes CMD+KEY+VALUE: " + totalBytesRead);
			System.out.println("--- State changed from VALUE -> CMD");

			error = true;
		break;

		default:
			System.out.println("Received Bytes Error: " + totalBytesRead);
			error = false;
			break;
		}

		return error;
	}
}
