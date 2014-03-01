package com.b6w7.eece411.P02;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
	private static final int TIMEOUT = 10000;		//Server Socket timeout Seconds

	
	//private static ArrayList<Socket> connected_clients;
	private static ConcurrentLinkedQueue<Command> connected_clients;
	
	public static void main(String[] args) throws IOException {
		
		connected_clients = new ConcurrentLinkedQueue<Command>();
		
		int servPort = Integer.parseInt(args[0]);

		Socket clientSock = null;
		
		int recvMsgSize = 0;   // Size of received message
		int totalBytesReceived = 0;   // Size of received message

		byte[] byteBufferIn = new byte[REQSIZE];  // Receive buffer
		char[] charBufferIn = new char[REQSIZE];  // Receive buffer for BufferedReader inFromClient requires a char[]
		byte[] byteBufferOut = new byte[RESSIZE]; // Response buffer
		
		try{
		
		// Create a server socket to accept client connection requests
		ServerSocket serverSock = new ServerSocket(servPort);
		//serverSock.setSoTimeout(TIMEOUT);
		//serverSock.setReceiveBufferSize(REQSIZE);
		//serverSock.setPerformancePreferences(connectionTime, latency, bandwidth);
		
		System.out.println("Listening for connections...");

		for (;;) { // Run forever, accepting and servicing connections
		
		// Thread 1 accepting connections			

			clientSock = serverSock.accept();     // Get client connection, Blocking call
			
			System.out.println("Handling client at " +
					clientSock.getInetAddress().getHostAddress());

			//InputStream in = clientSock.getInputStream();
			//OutputStream out = clientSock.getOutputStream();
			DataOutputStream outToClient = 
					new DataOutputStream(clientSock.getOutputStream());
			BufferedInputStream inFromClient = new BufferedInputStream(clientSock.getInputStream() );

			// InputStream inFromClient = clientSock.getInputStream();
			
			while ( !clientSock.isClosed() ) {
				charBufferIn = new char[REQSIZE]; 
				byteBufferIn = new byte[REQSIZE];
				byteBufferOut = new byte[RESSIZE];
				recvMsgSize = totalBytesReceived = 0;

			Command ci = new Command(clientSock);
			
			// Add client socket to the queue of connected clients.
			if( !connected_clients.offer(ci) ){
				System.out.println("Max connected clients.\n");
				clientSock.close();
				continue;
			}
			
			try {
				// Receive fields "Command" and "Keys" number of bytes or 
				// until client closes connection, indicated by -1 return 
					recvMsgSize = inFromClient.read(byteBufferIn, 0, CMDSIZE+KEYSIZE-totalBytesReceived);
					totalBytesReceived += recvMsgSize;
					if ( !checkBytes(totalBytesReceived) ){
						throw new SocketException("Incorrect bytes received CMD+KEY: " + totalBytesReceived);
						
				}	
			} catch (IOException e1) {
				// Error in reading from input stream.
				e1.printStackTrace();
				byteBufferIn = null;
				throw new DataFormatException("Error in reading from input stream. Bytes read: " + totalBytesReceived);
			}			

			System.out.println("Sucessfully read CMD+KEY...");

			//Parse and Extract relevant data
			byte cmd;
			byte[] key;
			byte[] value;
			
			ByteBuffer dataRead = ByteBuffer.wrap(byteBufferIn);
			cmd = dataRead.get();
			key = new byte[KEYSIZE];		dataRead.get(key, CMDSIZE, key.length-1);


			// Read field "Value" only if the command received is "put".
			if( cmd == (byte) Request.CMD_PUT.getCode() ){
				try {
					// Receive field "Value" number of bytes or 
					// until client closes connection, indicated by -1 return 
						recvMsgSize = inFromClient.read(byteBufferIn, CMDSIZE+KEYSIZE, VALUESIZE);
						totalBytesReceived += recvMsgSize;
						if ( !checkBytes(totalBytesReceived) ){
							System.out.println("Incorrect bytes received VALUE: " + totalBytesReceived);
							//throw new DataFormatException("Incorrect bytes received VALUE: " + totalBytesReceived);
						}	
							
				} catch (IOException e1) {
					// Error in reading from input stream.
					e1.printStackTrace();
					byteBufferIn = null;
					throw new DataFormatException("Error in reading from input stream. Bytes read: " + totalBytesReceived);
				}	
			}
			dataRead = ByteBuffer.wrap(	byteBufferIn );
			value = Arrays.copyOfRange(dataRead.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);

			System.out.println("Sucessfully read CMD+KEY+VALUE...");
			StringBuilder s = new StringBuilder();
			for (int i=1; i<(1+32); i++) {
				s.append(Integer.toString((dataRead.array()[i] & 0xff) + 0x100, 16).substring(1));
			}
			s.append(" ");
			/*for (int i=33; i<dataRead.limit(); i++) {
				s.append(Integer.toString((dataRead.array()[i] & 0xff) + 0x100, 16).substring(1));
			}
			*/
			byte valueArrayTemp[] = Arrays.copyOfRange(dataRead.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
			try {
				// s.append(new String(value.array(), StandardCharsets.UTF_8.displayName()));
				s.append(new String(valueArrayTemp, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				s.append(new String(valueArrayTemp));
			}
			System.out.println("Request Received: "+s.toString());
			
			// set the request received to associated client.
			ci.setRequest(cmd, ByteBuffer.wrap(key), ByteBuffer.wrap(value) );
			
			//Execute received command.
			
			// TODO: Currently ClientInterface.getReply() will execute the command before generating a reply.
			// getReply() is called from the Thread sending replies.

						
			//Obtain a connected client and reply to the client with its response.
			Command clientToReply =	connected_clients.poll();

			// Send reply to client
			if(clientToReply != null){
				try {
					System.out.println("Writing Response.");

					byteBufferOut= clientToReply.getReply().array();
					outToClient.write(byteBufferOut, 0, byteBufferOut.length);
					System.out.println("Total elements in map: "+ Command.numElements);

				} catch (IOException e1) {
					// TODO:
					// Error in writing to output stream.
					e1.printStackTrace();
				}
				System.out.println("Closing socket. Written bytes: "+byteBufferOut.length);
				clientToReply.getSocket().close(); // Close the socket.  We are done with this client!
			}
			
			
			//clientSock.close();  // Close the socket.  We are done with this client!
			}
		}
		
	} catch (DataFormatException e) {
		//Error in reading from input stream
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		System.out.println("Network error occurred."+e.getLocalizedMessage());
		
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
