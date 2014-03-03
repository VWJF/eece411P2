package com.b6w7.eece411.P02;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.DataFormatException;

import com.b6w7.eece411.P02.NodeCommands.Reply;
import com.b6w7.eece411.P02.NodeCommands.Request;

public class Launcher0 {

	// number of bytes in protocol field
	private static final int CMDSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int KEYSIZE = NodeCommands.LEN_KEY_BYTES;
	private static final int VALUESIZE = NodeCommands.LEN_VALUE_BYTES;
	
	// Size of protocol buffers
	private static final int REQSIZE = CMDSIZE+KEYSIZE+VALUESIZE;  // request buffer
	private static final int RESSIZE = CMDSIZE+VALUESIZE;   //response buffer

	private static final int NUM_CLIENTS = 15; 	//Max number or open connections to planetlab = 30
	private static final int TIMEOUT = 10000;		//Server Socket timeout Seconds

	
	//private static ArrayList<Socket> connected_clients;
	private static ConcurrentLinkedQueue<Command> connected_clients;
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		connected_clients = new ConcurrentLinkedQueue<Command>();
		
		int servPort = Integer.parseInt(args[0]);
		
		int attempt = 0; //
		
		Socket clientSocket = null;
		
		int recvMsgSize = 0;   // Size of received message
		int totalBytesReceived = 0;   // Size of received message

		byte[] byteBufferIn = new byte[REQSIZE];  // Receive buffer
		byte[] byteBufferOut = new byte[RESSIZE]; // Response buffer
		
		try{
		
		// Create a server socket to accept client connection requests
		ServerSocket serverSock = new ServerSocket(servPort);
		//serverSock.setSoTimeout(TIMEOUT);
		//serverSock.setReceiveBufferSize(REQSIZE);
		//serverSock.setPerformancePreferences(connectionTime, latency, bandwidth);
		
		System.out.println("Listening for connections...");

		boolean end_of_loop = false;
		for (;;) { // Run forever, accepting and servicing connections
		
			System.out.println();
			System.out.println("Accepting clients...");
			clientSocket = serverSock.accept();     // Get client connection, Blocking call
			
			System.out.println("Handling client at " +
					clientSocket.getInetAddress().getHostAddress());

			DataOutputStream outToClient = 
					new DataOutputStream(clientSocket.getOutputStream());
			BufferedInputStream inFromClient = 
					new BufferedInputStream(clientSocket.getInputStream() );
			
			// Add client socket to the queue of connected clients.
			//if( !connected_clients.offer(ci) ){
			//	System.out.println("Max connected clients.\n");
			//	clientSocket.close();
			//	continue READLOOP;
			//}
			end_of_loop = false;

			ClientLoop:while ( !end_of_loop && (recvMsgSize = inFromClient.read(byteBufferIn, 0, CMDSIZE)) > -1 ) {		
				System.out.println("\n--- Attempt: "+(attempt++));
				
				totalBytesReceived += recvMsgSize;
				System.out.println("Sucessfully read CMD... "+totalBytesReceived+"bytes");
				end_of_loop = false;
				
			try {
				// Receive fields "Command" and "Keys" number of bytes or 
				// until client closes connection, indicated by -1 return 
				recvMsgSize = inFromClient.read(byteBufferIn, CMDSIZE, KEYSIZE);
				totalBytesReceived += recvMsgSize;
				if( recvMsgSize == -1 ){
					end_of_loop = true;
					System.out.println("Incorrect bytes received KEYSIZE: " + totalBytesReceived);
					continue ClientLoop;
					//throw new DataFormatException("Incorrect bytes received VALUE: " + totalBytesReceived);
				}	
			} catch (IOException e1) {
				// Error in reading from input stream.
				e1.printStackTrace();
				byteBufferIn = new byte[REQSIZE];
				byteBufferOut = new byte[RESSIZE];
				recvMsgSize = totalBytesReceived = 0;
				throw new DataFormatException("Error in reading for KEY from input stream. Bytes read: " + totalBytesReceived);
			}			

			System.out.println("Sucessfully read CMD+KEY... "+totalBytesReceived+"bytes");

			//Parse and Extract relevant data
			byte cmd;
			byte[] key;
			byte[] value;
			
			ByteBuffer dataRead = ByteBuffer.wrap(byteBufferIn);
			cmd = dataRead.get();
			key = new byte[KEYSIZE];		
			dataRead.get(key, CMDSIZE, key.length-1);

			// Read field "Value" only if the command received is "put".
			if( cmd == (byte) Request.CMD_PUT.getCode() ){
				try {
					// Receive field "Value" number of bytes or 
					// until client closes connection, indicated by -1 return 
					recvMsgSize = inFromClient.read(byteBufferIn, CMDSIZE+KEYSIZE, VALUESIZE);
					totalBytesReceived += recvMsgSize;
					if( recvMsgSize == -1 ){
						end_of_loop = true;
						System.out.println("Incorrect bytes received VALUE: " + totalBytesReceived);
						continue ClientLoop;
						//throw new DataFormatException("Incorrect bytes received VALUE: " + totalBytesReceived);
					}				
				} catch (IOException e1) {
					// Error in reading from input stream.
					e1.printStackTrace();
					byteBufferIn = new byte[REQSIZE];
					byteBufferOut = new byte[RESSIZE];
					recvMsgSize = totalBytesReceived = 0;
					throw new DataFormatException("Error in reading for VALUE from input stream. Bytes read: " + totalBytesReceived);
				}	
			}
			
			System.out.println("Sucessfully read CMD+KEY+VALUE... "+totalBytesReceived+"bytes");

			dataRead = ByteBuffer.wrap(	byteBufferIn );
			value = Arrays.copyOfRange(dataRead.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);


			String s = NodeCommands.requestByteArrayToString(dataRead.array());
			System.out.println("Request Received(cmd,key,value): "+s.toString());
			System.out.println("Request Received(cmd,key,value): ("+cmd+", "+key+", "+value.toString()+") ");

			Command clientToReply = new Command(clientSocket, cmd, ByteBuffer.wrap(key), ByteBuffer.wrap(value) );

						
			//Obtain a connected client and reply to the client with its response.
			//Command clientToReply =	connected_clients.poll();

			// Send reply to client
			if(clientToReply != null){
				try {
					System.out.println("Writing Response.");

					byteBufferOut= clientToReply.getReply().array();
					String p = new String(byteBufferOut);
					String q = NodeCommands.byteArrayAsString(byteBufferOut);
					outToClient.write(byteBufferOut, 0, byteBufferOut.length);
					System.out.println("Total elements in map: "+ Command.getNumElements());
					System.out.println("All Bytes Written(string,array): ("+ p+", "+q.substring(0, 2)+" "+q.substring(2)+")");
					System.out.println("Expected Bytes in response, Total Bytes written in socket: (" + p.length()+ ", " +outToClient.size()+")");

				} catch (IOException e1) {
					// TODO:
					// Error in writing to output stream.
					e1.printStackTrace();
				}
				//System.out.println("Closing socket. Written bytes: "+byteBufferOut.length);
				//clientToReply.getSocket().close(); // Close the socket.  We are done with this client!
				System.out.println("Completed Processing.");
				
				System.out.println("\tAbout socket: "+clientSocket.toString());
				System.out.println("\tSoTimeout: "+clientSocket.getSoTimeout()+
									", isClosed: "+clientSocket.isClosed()+
									", isInputShutdown: "+clientSocket.isInputShutdown()+
									", isOutputShutdown "+clientSocket.isOutputShutdown()+
									", getSendBufferSize "+clientSocket.getSendBufferSize()+
									", getReceiveBufferSize "+clientSocket.getReceiveBufferSize()
									);
				System.out.println("\tend of loop: "+end_of_loop);

			}
			
			byteBufferIn = new byte[REQSIZE];
			byteBufferOut = new byte[RESSIZE];
			recvMsgSize = totalBytesReceived = 0;
			}
			
			System.out.println("\tAbout socket: "+clientSocket.toString());
			System.out.println("\tSoTimeout: "+clientSocket.getSoTimeout()+
								", isClosed: "+clientSocket.isClosed()+
								", isInputShutdown: "+clientSocket.isInputShutdown()+
								", isOutputShutdown "+clientSocket.isOutputShutdown()+
								", getSendBufferSize "+clientSocket.getSendBufferSize()+
								", getReceiveBufferSize "+clientSocket.getReceiveBufferSize()
								);
			System.out.println("\tend of loop: "+end_of_loop);
			System.out.println("Closing socket.");
			clientSocket.close();
		}
		
	} catch (DataFormatException e) {
		//Error in reading from input stream
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		System.out.println("Network error occurred."+e.getLocalizedMessage());
		
	} finally {
		if (null != clientSocket) {
			try {
				System.out.println("finally reached.");
				System.out.println("Closing Client.");
				// Close the socket. We are done with this client!
				clientSocket.close();
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
		boolean error_code = false;

		switch (totalBytesRead) {
		case CMDSIZE: // Field "Command" has been read.
			System.out.println("Received Bytes in CMD: " + totalBytesRead);

			//System.out.println("Received Cmd: " + Integer.toString((byteBuffer[0] & 0xff) + 0x100, 16).substring(1));
			System.out.println("--- State changed from CMD -> KEY");
			error_code = true;
			break;

		case CMDSIZE+KEYSIZE: // Field "KEY" has been read.
			System.out.println("Received Bytes in CMD+KEY: " + totalBytesRead);

			//System.out.println("Received Key: " + totalBytesRead);
			System.out.println("--- State changed from KEY -> VALUE");
			error_code = true;
		break;

		case CMDSIZE+KEYSIZE+VALUESIZE: // Field "VALUE" has been read.

			System.out.println("Received Bytes CMD+KEY+VALUE: " + totalBytesRead);
			System.out.println("--- State changed from VALUE -> CMD");

			error_code = true;
		break;

		default:
			System.out.println("Received Bytes Error: " + totalBytesRead);
			error_code = false;
			break;
		}
		System.out.println("Error Code: " + error_code);

		return error_code;
	}
	
	

	
}
